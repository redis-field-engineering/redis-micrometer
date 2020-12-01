package com.redislabs.redistimeseries.micrometer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redislabs.mesclun.RedisTimeSeriesClient;
import com.redislabs.mesclun.api.StatefulRedisTimeSeriesConnection;
import com.redislabs.mesclun.api.async.RedisTimeSeriesAsyncCommands;
import com.redislabs.mesclun.timeseries.Label;

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
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.cumulative.CumulativeCounter;
import io.micrometer.core.instrument.cumulative.CumulativeFunctionCounter;
import io.micrometer.core.instrument.cumulative.CumulativeFunctionTimer;
import io.micrometer.core.instrument.distribution.CountAtBucket;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.internal.CumulativeHistogramLongTaskTimer;
import io.micrometer.core.instrument.internal.DefaultGauge;
import io.micrometer.core.instrument.internal.DefaultMeter;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.util.MeterPartition;
import io.micrometer.core.instrument.util.NamedThreadFactory;
import io.micrometer.core.lang.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link MeterRegistry} for RedisTimeSeries.
 *
 * @author Julien Ruaux
 */
@Slf4j
public class RedisTimeSeriesMeterRegistry extends StepMeterRegistry {
	private static final ThreadFactory DEFAULT_THREAD_FACTORY = new NamedThreadFactory(
			"redistimeseries-metrics-publisher");

	private final RedisTimeSeriesConfig config;
	private final RedisTimeSeriesClient client;
	private final GenericObjectPool<StatefulRedisTimeSeriesConnection<String, String>> pool;

	public RedisTimeSeriesMeterRegistry(RedisTimeSeriesConfig config, Clock clock) {
		this(config, clock, DEFAULT_THREAD_FACTORY);
	}

	public RedisTimeSeriesMeterRegistry(RedisTimeSeriesConfig config, Clock clock, ThreadFactory threadFactory) {
		this(config, clock, threadFactory, RedisTimeSeriesClient.create(config.uri()));
	}

	public RedisTimeSeriesMeterRegistry(RedisTimeSeriesConfig config, Clock clock, ThreadFactory threadFactory,
			RedisTimeSeriesClient client) {
		super(config, clock);
		config().namingConvention(new RedisTimeSeriesNamingConvention());
		this.config = config;
		this.client = client;
		this.pool = ConnectionPoolSupport.createGenericObjectPool(client::connect, poolConfig(config));
		start(threadFactory);
	}

	@Override
	public void close() {
		super.close();
		pool.close();
		client.shutdown();
	}

	private GenericObjectPoolConfig<StatefulRedisTimeSeriesConnection<String, String>> poolConfig(
			RedisTimeSeriesConfig config) {
		GenericObjectPoolConfig<StatefulRedisTimeSeriesConnection<String, String>> poolConfig = new GenericObjectPoolConfig<>();
		poolConfig.setMaxTotal(config.poolMaxTotal());
		return poolConfig;
	}

	public static Builder builder(RedisTimeSeriesConfig config) {
		return new Builder(config);
	}

	@Override
	public Counter newCounter(Meter.Id id) {
		return new CumulativeCounter(id);
	}

	@Override
	protected <T> Gauge newGauge(Meter.Id id, @Nullable T obj, ToDoubleFunction<T> valueFunction) {
		return new DefaultGauge<>(id, obj, valueFunction);
	}

	@Override
	protected LongTaskTimer newLongTaskTimer(Meter.Id id, DistributionStatisticConfig distributionStatisticConfig) {
		return new CumulativeHistogramLongTaskTimer(id, clock, getBaseTimeUnit(), distributionStatisticConfig);
	}

	@Override
	protected <T> FunctionTimer newFunctionTimer(Meter.Id id, T obj, ToLongFunction<T> countFunction,
			ToDoubleFunction<T> totalTimeFunction, TimeUnit totalTimeFunctionUnit) {
		return new CumulativeFunctionTimer<>(id, obj, countFunction, totalTimeFunction, totalTimeFunctionUnit,
				getBaseTimeUnit());
	}

	@Override
	protected <T> FunctionCounter newFunctionCounter(Meter.Id id, T obj, ToDoubleFunction<T> countFunction) {
		return new CumulativeFunctionCounter<>(id, obj, countFunction);
	}

	@Override
	protected Meter newMeter(Meter.Id id, Meter.Type type, Iterable<Measurement> measurements) {
		return new DefaultMeter(id, type, measurements);
	}

	@Override
	protected TimeUnit getBaseTimeUnit() {
		return TimeUnit.SECONDS;
	}

	@Override
	protected DistributionStatisticConfig defaultHistogramConfig() {
		return DistributionStatisticConfig.builder().expiry(config.step()).build()
				.merge(DistributionStatisticConfig.DEFAULT);
	}

	@Override
	protected void publish() {
		try (MetricsWriter writer = writer()) {
			for (List<Meter> batch : MeterPartition.partition(this, config.batchSize())) {
				try {
					writer.write(batch);
				} catch (InterruptedException e) {
					return;
				}
			}
		} catch (Exception e) {
			log.error("Could not get connection from pool", e);
		}

	}

	private MetricsWriter writer() throws Exception {
		return new MetricsWriter(pool.borrowObject());
	}

	public void write(Meter... batch) throws Exception {
		writer().write(Arrays.asList(batch));
	}

	public class MetricsWriter implements AutoCloseable {

		private final StatefulRedisTimeSeriesConnection<String, String> connection;
		private final RedisTimeSeriesAsyncCommands<String, String> commands;

		public MetricsWriter(StatefulRedisTimeSeriesConnection<String, String> connection) {
			this.connection = connection;
			this.commands = connection.async();
			this.commands.setAutoFlushCommands(false);
		}

		@Override
		public void close() throws Exception {
			this.commands.setAutoFlushCommands(true);
			this.connection.close();
		}

		public void write(List<Meter> batch) throws InterruptedException {
			List<RedisFuture<Long>> futures = new ArrayList<>(batch.size());
			for (Meter meter : batch) {
				futures.addAll(meter.match(this::writeGauge, this::writeCounter, this::writeTimer, this::writeSummary,
						this::writeLongTaskTimer, this::writeTimeGauge, this::writeFunctionCounter,
						this::writeFunctionTimer, this::writeCustomMetric));
			}
			commands.flushCommands();
			long commandTimeout = config.uri().getTimeout().getSeconds();
			for (RedisFuture<?> future : futures) {
				try {
					future.get(commandTimeout, TimeUnit.SECONDS);
				} catch (ExecutionException e) {
					log.error("Could not write item", e.getCause());
				} catch (TimeoutException e) {
					log.error("Command timed out", e);
				}
			}
		}

		// VisibleForTesting
		List<RedisFuture<Long>> writeGauge(Gauge gauge) {
			double value = gauge.value();
			if (Double.isFinite(value)) {
				return Arrays.asList(writeMetric(gauge.getId(), config().clock().wallTime(), value));
			}
			return Collections.emptyList();
		}

		List<RedisFuture<Long>> writeSummary(DistributionSummary summary) {
			List<RedisFuture<Long>> metrics = new ArrayList<>();
			long wallTime = config().clock().wallTime();
			metrics.add(writeMetricWithSuffix(summary.getId(), "count", wallTime, summary.count()));
			metrics.add(writeMetricWithSuffix(summary.getId(), "sum", wallTime, summary.totalAmount()));
			metrics.add(writeMetricWithSuffix(summary.getId(), "max", wallTime, summary.max()));
			metrics.add(writeMetricWithSuffix(summary.getId(), "mean", wallTime, summary.mean()));
			final ValueAtPercentile[] percentileValues = summary.takeSnapshot().percentileValues();
			if (percentileValues.length > 0) {
				metrics.addAll(writePercentiles(summary, wallTime, percentileValues));
			}
			return metrics;
		}

		List<RedisFuture<Long>> writeFunctionTimer(FunctionTimer timer) {
			long wallTime = config().clock().wallTime();

			return Arrays.asList(writeMetricWithSuffix(timer.getId(), "count", wallTime, timer.count()),
					// not applicable
					// writeMetricWithSuffix(timer.getId(), "avg", wallTime,
					// timer.mean(getBaseTimeUnit())),
					writeMetricWithSuffix(timer.getId(), "sum", wallTime, timer.totalTime(getBaseTimeUnit())));
		}

		List<RedisFuture<Long>> writeTimer(Timer timer) {
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

			return metrics;
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
				double count, @Nullable TimeUnit timeUnit) {
			List<RedisFuture<Long>> metrics = new ArrayList<>(histogramCounts.length);

			// satisfies https://prometheus.io/docs/concepts/metric_types/#histogram, which
			// is at least SOME standard
			// histogram format to follow
			for (CountAtBucket c : histogramCounts) {
				metrics.add(writeMetricWithSuffix(
						meter.getId()
								.withTag(new ImmutableTag("le",
										String.valueOf(timeUnit == null ? c.bucket() : c.bucket(timeUnit)))),
						"bucket", wallTime, c.count()));
			}

			// the +Inf bucket should always equal `count`
			metrics.add(writeMetricWithSuffix(meter.getId().withTag(new ImmutableTag("le", "+Inf")), "bucket", wallTime,
					count));
			return metrics;
		}

		// VisibleForTesting
		List<RedisFuture<Long>> writeFunctionCounter(FunctionCounter counter) {
			double count = counter.count();
			if (Double.isFinite(count)) {
				return Arrays.asList(writeMetric(counter.getId(), config().clock().wallTime(), count));
			}
			return Collections.emptyList();
		}

		List<RedisFuture<Long>> writeCounter(Counter counter) {
			return Arrays.asList(writeMetric(counter.getId(), config().clock().wallTime(), counter.count()));
		}

		// VisibleForTesting
		List<RedisFuture<Long>> writeTimeGauge(TimeGauge timeGauge) {
			double value = timeGauge.value(getBaseTimeUnit());
			if (Double.isFinite(value)) {
				return Arrays.asList(writeMetric(timeGauge.getId(), config().clock().wallTime(), value));
			}
			return Collections.emptyList();
		}

		List<RedisFuture<Long>> writeLongTaskTimer(LongTaskTimer timer) {
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

			return metrics;
		}

		@SuppressWarnings("incomplete-switch")
		private List<RedisFuture<Long>> writeCustomMetric(Meter meter) {
			long wallTime = config().clock().wallTime();

			List<Tag> tags = getConventionTags(meter.getId());

			return StreamSupport.stream(meter.measure().spliterator(), false).map(ms -> {
				Tags localTags = Tags.concat(tags, "statistics", ms.getStatistic().toString());
				String name = getConventionName(meter.getId());

				switch (ms.getStatistic()) {
				case TOTAL:
				case TOTAL_TIME:
					name += ":sum";
					break;
				case MAX:
					name += ":max";
					break;
				case ACTIVE_TASKS:
					name += ":active:count";
					break;
				case DURATION:
					name += ":duration:sum";
					break;
				}

				return commands.add(name, wallTime, ms.getValue(), labels(localTags));
			}).collect(Collectors.toList());
		}

		RedisFuture<Long> writeMetric(Meter.Id id, long wallTime, double value) {
			return writeMetricWithSuffix(id, "", wallTime, value);
		}

		RedisFuture<Long> writeMetricWithSuffix(Meter.Id id, String suffix, long wallTime, double value) {
			// usually tagKeys and metricNames naming rules are the same
			// but we can't call getConventionName again after adding suffix
			String key = suffix.isEmpty() ? getConventionName(id)
					: config().namingConvention().tagKey(getConventionName(id) + ":" + suffix);
			return commands.add(key, wallTime, value, labels(id));
		}
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

	public static class Builder {
		private final RedisTimeSeriesConfig config;

		private Clock clock = Clock.SYSTEM;
		private ThreadFactory threadFactory = DEFAULT_THREAD_FACTORY;
		private RedisTimeSeriesClient client;

		Builder(RedisTimeSeriesConfig config) {
			this.config = config;
			this.client = RedisTimeSeriesClient.create(config.uri());
		}

		public Builder clock(Clock clock) {
			this.clock = clock;
			return this;
		}

		public Builder threadFactory(ThreadFactory threadFactory) {
			this.threadFactory = threadFactory;
			return this;
		}

		public Builder client(RedisTimeSeriesClient client) {
			this.client = client;
			return this;
		}

		public RedisTimeSeriesMeterRegistry build() {
			return new RedisTimeSeriesMeterRegistry(config, clock, threadFactory, client);
		}
	}

}
