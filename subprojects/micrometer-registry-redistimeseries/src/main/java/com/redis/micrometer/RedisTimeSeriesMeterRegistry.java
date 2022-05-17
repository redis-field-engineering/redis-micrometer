package com.redis.micrometer;

import static io.micrometer.core.instrument.distribution.FixedBoundaryVictoriaMetricsHistogram.getRangeTagValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
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
import io.micrometer.core.instrument.Meter.Id;
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
			log.info("Client is null, skipping publish");
			return;
		}
		for (List<Meter> batch : MeterPartition.partition(this, config.batchSize())) {
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

	public void write(List<Meter> batch) throws Exception {
		if (batch.isEmpty()) {
			return;
		}
		try (StatefulRedisModulesConnection<String, String> connection = pool.borrowObject()) {
			RedisModulesAsyncCommands<String, String> commands = connection.async();
			log.info("Disabling command auto-flush");
			commands.setAutoFlushCommands(false);
			List<RedisFuture<Long>> futures = batch.stream()
					.flatMap(meter -> meter.match(m -> writeGauge(commands, m), m -> writeCounter(commands, m),
							m -> writeTimer(commands, m), m -> writeSummary(commands, m),
							m -> writeLongTaskTimer(commands, m), m -> writeTimeGauge(commands, m),
							m -> writeFunctionCounter(commands, m), m -> writeFunctionTimer(commands, m),
							m -> writeCustomMetric(commands, m)))
					.collect(Collectors.toList());
			commands.flushCommands();
			LettuceFutures.awaitAll(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS,
					futures.toArray(new Future[0]));
			commands.setAutoFlushCommands(true);
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

	Stream<RedisFuture<Long>> writeSummary(RedisTimeSeriesAsyncCommands<String, String> commands,
			DistributionSummary summary) {
		long wallTime = config().clock().wallTime();

		final ValueAtPercentile[] percentileValues = summary.takeSnapshot().percentileValues();
		double count = summary.count();

		List<RedisFuture<Long>> metrics = new ArrayList<>();

		metrics.add(writeMetricWithSuffix(commands, summary.getId(), "count", wallTime, count));
		metrics.add(writeMetricWithSuffix(commands, summary.getId(), "sum", wallTime, summary.totalAmount()));
		metrics.add(writeMetricWithSuffix(commands, summary.getId(), "max", wallTime, summary.max()));
		metrics.add(writeMetricWithSuffix(commands, summary.getId(), "mean", wallTime, summary.mean()));

		if (percentileValues.length > 0) {
			metrics.addAll(writePercentiles(commands, summary, wallTime, percentileValues));
		}

		return metrics.stream();
	}

	Stream<RedisFuture<Long>> writeFunctionTimer(RedisTimeSeriesAsyncCommands<String, String> commands,
			FunctionTimer timer) {
		long wallTime = config().clock().wallTime();

		return Stream.of(writeMetricWithSuffix(commands, timer.getId(), "count", wallTime, timer.count()),
				// not applicable
				// writeMetricWithSuffix(timer.getId(), "avg", wallTime,
				// timer.mean(getBaseTimeUnit())),
				writeMetricWithSuffix(commands, timer.getId(), "sum", wallTime, timer.totalTime(getBaseTimeUnit())));
	}

	Stream<RedisFuture<Long>> writeTimer(RedisTimeSeriesAsyncCommands<String, String> commands, Timer timer) {
		long wallTime = config().clock().wallTime();

		HistogramSnapshot histogramSnapshot = timer.takeSnapshot();
		final ValueAtPercentile[] percentileValues = histogramSnapshot.percentileValues();
		final CountAtBucket[] histogramCounts = histogramSnapshot.histogramCounts();
		double count = timer.count();

		List<RedisFuture<Long>> metrics = new ArrayList<>();

		metrics.add(writeMetricWithSuffix(commands, timer.getId(), "count", wallTime, count));
		metrics.add(
				writeMetricWithSuffix(commands, timer.getId(), "sum", wallTime, timer.totalTime(getBaseTimeUnit())));
		metrics.add(writeMetricWithSuffix(commands, timer.getId(), "max", wallTime, timer.max(getBaseTimeUnit())));

		if (percentileValues.length > 0) {
			metrics.addAll(writePercentiles(commands, timer, wallTime, percentileValues));
		}

		if (histogramCounts.length > 0) {
			metrics.addAll(writeHistogram(commands, wallTime, timer, histogramCounts, count, getBaseTimeUnit()));
		}

		return metrics.stream();
	}

	private List<RedisFuture<Long>> writePercentiles(RedisTimeSeriesAsyncCommands<String, String> commands, Meter meter,
			long wallTime, ValueAtPercentile[] percentileValues) {
		List<RedisFuture<Long>> metrics = new ArrayList<>(percentileValues.length);

		boolean forTimer = meter instanceof Timer;
		// satisfies https://prometheus.io/docs/concepts/metric_types/#summary
		for (ValueAtPercentile v : percentileValues) {
			metrics.add(writeMetric(commands,
					meter.getId().withTag(new ImmutableTag("quantile", String.valueOf(v.percentile()))), wallTime,
					(forTimer ? v.value(getBaseTimeUnit()) : v.value())));
		}

		return metrics;
	}

	private List<RedisFuture<Long>> writeHistogram(RedisTimeSeriesAsyncCommands<String, String> commands, long wallTime,
			Meter meter, CountAtBucket[] histogramCounts, double count, TimeUnit timeUnit) {
		List<RedisFuture<Long>> metrics = new ArrayList<>(histogramCounts.length);

		for (CountAtBucket c : histogramCounts) {
			metrics.add(writeMetricWithSuffix(commands,
					meter.getId().withTag(
							Tag.of("vmrange", getRangeTagValue(timeUnit == null ? c.bucket() : c.bucket(timeUnit)))),
					"bucket", wallTime, c.count()));
		}

		return metrics;
	}

	// VisibleForTesting
	Stream<RedisFuture<Long>> writeFunctionCounter(RedisTimeSeriesAsyncCommands<String, String> commands,
			FunctionCounter counter) {
		double count = counter.count();
		if (Double.isFinite(count)) {
			return Stream.of(writeMetric(commands, counter.getId(), config().clock().wallTime(), count));
		}
		return Stream.empty();
	}

	Stream<RedisFuture<Long>> writeCounter(RedisTimeSeriesAsyncCommands<String, String> commands, Counter counter) {
		return Stream.of(writeMetric(commands, counter.getId(), config().clock().wallTime(), counter.count()));
	}

	// VisibleForTesting
	Stream<RedisFuture<Long>> writeGauge(RedisTimeSeriesAsyncCommands<String, String> commands, Gauge gauge) {
		double value = gauge.value();
		if (Double.isFinite(value)) {
			return Stream.of(writeMetric(commands, gauge.getId(), config().clock().wallTime(), value));
		}
		return Stream.empty();
	}

	// VisibleForTesting
	Stream<RedisFuture<Long>> writeTimeGauge(RedisTimeSeriesAsyncCommands<String, String> commands,
			TimeGauge timeGauge) {
		double value = timeGauge.value(getBaseTimeUnit());
		if (Double.isFinite(value)) {
			return Stream.of(writeMetric(commands, timeGauge.getId(), config().clock().wallTime(), value));
		}
		return Stream.empty();
	}

	Stream<RedisFuture<Long>> writeLongTaskTimer(RedisTimeSeriesAsyncCommands<String, String> commands,
			LongTaskTimer timer) {
		long wallTime = config().clock().wallTime();

		HistogramSnapshot histogramSnapshot = timer.takeSnapshot();
		final ValueAtPercentile[] percentileValues = histogramSnapshot.percentileValues();
		final CountAtBucket[] histogramCounts = histogramSnapshot.histogramCounts();
		double count = timer.activeTasks();

		List<RedisFuture<Long>> metrics = new ArrayList<>();

		metrics.add(writeMetricWithSuffix(commands, timer.getId(), "active.count", wallTime, count));
		metrics.add(writeMetricWithSuffix(commands, timer.getId(), "duration.sum", wallTime,
				timer.duration(getBaseTimeUnit())));
		metrics.add(writeMetricWithSuffix(commands, timer.getId(), "max", wallTime, timer.max(getBaseTimeUnit())));

		if (percentileValues.length > 0) {
			metrics.addAll(writePercentiles(commands, timer, wallTime, percentileValues));
		}

		if (histogramCounts.length > 0) {
			metrics.addAll(writeHistogram(commands, wallTime, timer, histogramCounts, count, getBaseTimeUnit()));
		}

		return metrics.stream();
	}

	@SuppressWarnings("incomplete-switch")
	Stream<RedisFuture<Long>> writeCustomMetric(RedisTimeSeriesAsyncCommands<String, String> commands, Meter meter) {
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

	RedisFuture<Long> writeMetricWithSuffix(RedisTimeSeriesAsyncCommands<String, String> commands, Meter.Id id,
			String suffix, long wallTime, double value) {
		// usually tagKeys and metricNames naming rules are the same
		// but we can't call getConventionName again after adding suffix
		return commands.add(
				suffix.isEmpty() ? getConventionName(id)
						: config().namingConvention().tagKey(getConventionName(id) + "." + suffix),
				wallTime, value, createOptions(labels(id)));
	}

	@Override
	protected String getConventionName(Id id) {
		StringBuilder hierarchicalName = new StringBuilder();
		hierarchicalName.append(super.getConventionName(id));
		for (Tag tag : id.getTagsAsIterable()) {
			hierarchicalName.append(RedisTimeSeriesNamingConvention.KEY_SEPARATOR)
					.append(config().namingConvention().tagKey(tag.getKey()))
					.append(RedisTimeSeriesNamingConvention.KEY_SEPARATOR)
					.append(config().namingConvention().tagValue(tag.getValue()));
		}
		return hierarchicalName.toString();

	}

	private CreateOptions<String, String> createOptions(Label<String, String>[] labels) {
		return CreateOptions.<String, String>builder().labels(labels).build();
	}

	RedisFuture<Long> writeMetric(RedisTimeSeriesAsyncCommands<String, String> commands, Meter.Id id, long wallTime,
			double value) {
		return writeMetricWithSuffix(commands, id, "", wallTime, value);
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
