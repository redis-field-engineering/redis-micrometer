package com.redis.micrometer;

import static io.micrometer.core.instrument.distribution.FixedBoundaryVictoriaMetricsHistogram.getRangeTagValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.Label;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.support.ConnectionPoolSupport;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.CountAtBucket;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.step.StepDistributionSummary;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.step.StepTimer;
import io.micrometer.core.instrument.util.MeterPartition;
import io.micrometer.core.instrument.util.NamedThreadFactory;

/**
 * {@link StepMeterRegistry} for RedisTimeSeries.
 *
 * @author Julien Ruaux
 */
public class RedisTimeSeriesMeterRegistry extends StepMeterRegistry {

	private static final ThreadFactory DEFAULT_THREAD_FACTORY = new NamedThreadFactory(
			"redistimeseries-metrics-publisher");

	private final Logger log = LoggerFactory.getLogger(RedisTimeSeriesMeterRegistry.class);

	private final RedisTimeSeriesConfig config;
	private AbstractRedisClient client;
	private GenericObjectPool<StatefulRedisModulesConnection<String, String>> pool;

	public RedisTimeSeriesMeterRegistry(RedisTimeSeriesConfig config, Clock clock) {
		this(config, clock, DEFAULT_THREAD_FACTORY);
	}

	private RedisTimeSeriesMeterRegistry(RedisTimeSeriesConfig config, Clock clock, ThreadFactory threadFactory) {
		super(config, clock);
		this.config = config;
		config().namingConvention(new RedisTimeSeriesNamingConvention());
		start(threadFactory);
	}

	private Supplier<StatefulRedisModulesConnection<String, String>> connectionSupplier(AbstractRedisClient client) {
		if (client instanceof RedisModulesClusterClient) {
			return ((RedisModulesClusterClient) client)::connect;
		}
		return ((RedisModulesClient) client)::connect;
	}

	private static GenericObjectPoolConfig<StatefulRedisModulesConnection<String, String>> poolConfig(
			RedisTimeSeriesConfig config) {
		GenericObjectPoolConfig<StatefulRedisModulesConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(config.poolMaxTotal());
		return poolConfig;
	}

	private static AbstractRedisClient client(RedisTimeSeriesConfig config) {
		if (config.cluster()) {
			return RedisModulesClusterClient.create(config.uri());
		}
		return RedisModulesClient.create(config.uri());
	}

	@Override
	public void start(ThreadFactory threadFactory) {
		if (config.enabled()) {
			client = client(config);
			pool = ConnectionPoolSupport.createGenericObjectPool(connectionSupplier(client), poolConfig(config));
			super.start(threadFactory);
		}
	}

	@Override
	public void stop() {
		if (pool != null) {
			pool.close();
		}
		if (client != null) {
			client.shutdown();
			client.getResources().shutdown();
		}
		super.stop();
	}

	@Override
	protected void publish() {
		if (client == null) {
			return;
		}
		List<List<Meter>> partitions = MeterPartition.partition(this, config.batchSize());
		for (List<Meter> batch : partitions) {
			try {
				write(batch);
			} catch (Exception e) {
				log.error("Could not get connection from pool", e);
			}
		}
	}

	public void write(Meter... meters) throws Exception {
		write(Arrays.asList(meters));
	}

	private void write(List<Meter> batch) throws Exception {
		try (MeterWriter writer = new MeterWriter(pool.borrowObject())) {
			for (Meter meter : batch) {
				meter.match(writer::writeGauge, writer::writeCounter, writer::writeTimer, writer::writeSummary,
						writer::writeLongTaskTimer, writer::writeTimeGauge, writer::writeFunctionCounter,
						writer::writeFunctionTimer, writer::writeCustomMetric);
			}
		}
	}

	@Override
	protected DistributionSummary newDistributionSummary(Meter.Id id,
			DistributionStatisticConfig distributionStatisticConfig, double scale) {
		return new StepDistributionSummary(id, clock, distributionStatisticConfig, scale, config.step().toMillis(),
				true);
	}

	@Override
	protected Timer newTimer(Meter.Id id, DistributionStatisticConfig distributionStatisticConfig,
			PauseDetector pauseDetector) {
		return new StepTimer(id, clock, distributionStatisticConfig, pauseDetector, getBaseTimeUnit(),
				this.config.step().toMillis(), true);
	}

	@Override
	protected TimeUnit getBaseTimeUnit() {
		return TimeUnit.MILLISECONDS;
	}

	private class MeterWriter implements AutoCloseable {

		private final StatefulRedisModulesConnection<String, String> connection;
		private final RedisModulesAsyncCommands<String, String> commands;
		private final List<RedisFuture<Long>> futures;

		public MeterWriter(StatefulRedisModulesConnection<String, String> connection) {
			this.connection = connection;
			this.commands = connection.async();
			this.commands.setAutoFlushCommands(false);
			this.futures = new ArrayList<>();
		}

		@Override
		public void close() throws Exception {
			commands.flushCommands();
			LettuceFutures.awaitAll(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS,
					futures.toArray(new Future[0]));
			commands.setAutoFlushCommands(true);
			connection.close();
		}

		Stream<RedisFuture<Long>> writeSummary(DistributionSummary summary) {
			long wallTime = config().clock().wallTime();

			final ValueAtPercentile[] percentileValues = summary.takeSnapshot().percentileValues();
			double count = summary.count();

			List<RedisFuture<Long>> metrics = new ArrayList<>();

			metrics.add(writeMetricWithSuffix(summary.getId(), "count", wallTime, count));
			metrics.add(writeMetricWithSuffix(summary.getId(), "sum", wallTime, summary.totalAmount()));
			metrics.add(writeMetricWithSuffix(summary.getId(), "max", wallTime, summary.max()));
			metrics.add(writeMetricWithSuffix(summary.getId(), "mean", wallTime, summary.mean()));

			if (percentileValues.length > 0) {
				metrics.addAll(writePercentiles(summary, wallTime, percentileValues));
			}

			return metrics.stream();
		}

		Stream<RedisFuture<Long>> writeFunctionTimer(FunctionTimer timer) {
			long wallTime = config().clock().wallTime();

			return Stream.of(writeMetricWithSuffix(timer.getId(), "count", wallTime, timer.count()),
					// not applicable
					// writeMetricWithSuffix(timer.getId(), "avg", wallTime,
					// timer.mean(getBaseTimeUnit())),
					writeMetricWithSuffix(timer.getId(), "sum", wallTime, timer.totalTime(getBaseTimeUnit())));
		}

		Stream<RedisFuture<Long>> writeTimer(Timer timer) {
			long wallTime = config().clock().wallTime();

			HistogramSnapshot histogramSnapshot = timer.takeSnapshot();
			final ValueAtPercentile[] percentileValues = histogramSnapshot.percentileValues();
			final CountAtBucket[] histogramCounts = histogramSnapshot.histogramCounts();
			double count = timer.count();

			List<RedisFuture<Long>> metrics = new ArrayList<>();

			metrics.add(writeMetricWithSuffix(timer.getId(), "count", wallTime, count));
			metrics.add(writeMetricWithSuffix(timer.getId(), "sum", wallTime, timer.totalTime(getBaseTimeUnit())));
			metrics.add(writeMetricWithSuffix(timer.getId(), "max", wallTime, timer.max(getBaseTimeUnit())));

			if (percentileValues.length > 0) {
				metrics.addAll(writePercentiles(timer, wallTime, percentileValues));
			}

			if (histogramCounts.length > 0) {
				metrics.addAll(writeHistogram(wallTime, timer, histogramCounts, count, getBaseTimeUnit()));
			}

			return metrics.stream();
		}

		private List<RedisFuture<Long>> writePercentiles(Meter meter, long wallTime,
				ValueAtPercentile[] percentileValues) {
			List<RedisFuture<Long>> metrics = new ArrayList<>(percentileValues.length);

			boolean forTimer = meter instanceof Timer;
			// satisfies https://prometheus.io/docs/concepts/metric_types/#summary
			for (ValueAtPercentile v : percentileValues) {
				metrics.add(
						writeMetric(meter.getId().withTag(new ImmutableTag("quantile", String.valueOf(v.percentile()))),
								wallTime, (forTimer ? v.value(getBaseTimeUnit()) : v.value())));
			}

			return metrics;
		}

		private List<RedisFuture<Long>> writeHistogram(long wallTime, Meter meter, CountAtBucket[] histogramCounts,
				double count, TimeUnit timeUnit) {
			List<RedisFuture<Long>> metrics = new ArrayList<>(histogramCounts.length);

			for (CountAtBucket c : histogramCounts) {
				metrics.add(writeMetricWithSuffix(
						meter.getId()
								.withTag(Tag.of("vmrange",
										getRangeTagValue(timeUnit == null ? c.bucket() : c.bucket(timeUnit)))),
						"bucket", wallTime, c.count()));
			}

			return metrics;
		}

		// VisibleForTesting
		Stream<RedisFuture<Long>> writeFunctionCounter(FunctionCounter counter) {
			double count = counter.count();
			if (Double.isFinite(count)) {
				return Stream.of(writeMetric(counter.getId(), config().clock().wallTime(), count));
			}
			return Stream.empty();
		}

		Stream<RedisFuture<Long>> writeCounter(Counter counter) {
			return Stream.of(writeMetric(counter.getId(), config().clock().wallTime(), counter.count()));
		}

		// VisibleForTesting
		Stream<RedisFuture<Long>> writeGauge(Gauge gauge) {
			double value = gauge.value();
			if (Double.isFinite(value)) {
				return Stream.of(writeMetric(gauge.getId(), config().clock().wallTime(), value));
			}
			return Stream.empty();
		}

		// VisibleForTesting
		Stream<RedisFuture<Long>> writeTimeGauge(TimeGauge timeGauge) {
			double value = timeGauge.value(getBaseTimeUnit());
			if (Double.isFinite(value)) {
				return Stream.of(writeMetric(timeGauge.getId(), config().clock().wallTime(), value));
			}
			return Stream.empty();
		}

		Stream<RedisFuture<Long>> writeLongTaskTimer(LongTaskTimer timer) {
			long wallTime = config().clock().wallTime();

			HistogramSnapshot histogramSnapshot = timer.takeSnapshot();
			final ValueAtPercentile[] percentileValues = histogramSnapshot.percentileValues();
			final CountAtBucket[] histogramCounts = histogramSnapshot.histogramCounts();
			double count = timer.activeTasks();

			List<RedisFuture<Long>> metrics = new ArrayList<>();

			metrics.add(writeMetricWithSuffix(timer.getId(), "active.count", wallTime, count));
			metrics.add(
					writeMetricWithSuffix(timer.getId(), "duration.sum", wallTime, timer.duration(getBaseTimeUnit())));
			metrics.add(writeMetricWithSuffix(timer.getId(), "max", wallTime, timer.max(getBaseTimeUnit())));

			if (percentileValues.length > 0) {
				metrics.addAll(writePercentiles(timer, wallTime, percentileValues));
			}

			if (histogramCounts.length > 0) {
				metrics.addAll(writeHistogram(wallTime, timer, histogramCounts, count, getBaseTimeUnit()));
			}

			return metrics.stream();
		}

		@SuppressWarnings("incomplete-switch")
		private Stream<RedisFuture<Long>> writeCustomMetric(Meter meter) {
			long wallTime = config().clock().wallTime();

			List<Tag> tags = getConventionTags(meter.getId());

			return StreamSupport.stream(meter.measure().spliterator(), false).map(ms -> {
				Tags localTags = Tags.concat(tags, "statistics", ms.getStatistic().toString());
				String name = getConventionName(meter.getId());

				switch (ms.getStatistic()) {
				case TOTAL:
				case TOTAL_TIME:
					name += ".sum";
					break;
				case MAX:
					name += ".max";
					break;
				case ACTIVE_TASKS:
					name += ".active.count";
					break;
				case DURATION:
					name += ".duration.sum";
					break;
				}

				return commands.add(name, wallTime, ms.getValue(), createOptions(labels(localTags)));
			});
		}

		RedisFuture<Long> writeMetricWithSuffix(Meter.Id id, String suffix, long wallTime, double value) {
			// usually tagKeys and metricNames naming rules are the same
			// but we can't call getConventionName again after adding suffix
			return commands.add(
					suffix.isEmpty() ? getConventionName(id)
							: config().namingConvention().tagKey(getConventionName(id) + "." + suffix),
					wallTime, value, createOptions(labels(id)));
		}

		private CreateOptions<String, String> createOptions(Label<String, String>[] labels) {
			return CreateOptions.<String, String>builder().labels(labels).build();
		}

		RedisFuture<Long> writeMetric(Meter.Id id, long wallTime, double value) {
			return writeMetricWithSuffix(id, "", wallTime, value);
		}

		private Label<String, String>[] labels(Meter.Id id) {
			return labels(getConventionTags(id));
		}

		@SuppressWarnings("unchecked")
		private Label<String, String>[] labels(Iterable<Tag> tags) {
			List<Label<String, String>> labels = new ArrayList<>();
			for (Tag tag : tags) {
				labels.add(Label.of(tag.getKey(), tag.getValue()));
			}
			return labels.toArray(new Label[0]);
		}
	}

}
