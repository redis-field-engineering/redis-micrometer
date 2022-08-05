package com.redis.micrometer;

import static io.micrometer.core.instrument.distribution.FixedBoundaryVictoriaMetricsHistogram.getRangeTagValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.async.RedisTimeSeriesAsyncCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.Label;
import com.redis.lettucemod.timeseries.Sample;

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
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.Tag;
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
import io.micrometer.core.instrument.util.TimeUtils;

/**
 * {@link StepMeterRegistry} for RedisTimeSeries.
 *
 * @author Julien Ruaux
 */
public class RedisTimeSeriesMeterRegistry extends StepMeterRegistry {

	private static final ThreadFactory DEFAULT_THREAD_FACTORY = new NamedThreadFactory(
			"redistimeseries-metrics-publisher");

	private static final String SUFFIX_COUNT = "count";
	private static final String SUFFIX_SUM = "sum";
	private static final String SUFFIX_MAX = "max";
	private static final String SUFFIX_MEAN = "mean";
	private static final String SUFFIX_DURATION_SUM = "duration.sum";
	private static final String SUFFIX_ACTIVE_COUNT = "active.count";
	private static final String SUFFIX_BUCKET = "bucket";

	private final Logger log = Logger.getLogger(RedisTimeSeriesMeterRegistry.class.getName());

	private final RedisTimeSeriesConfig config;
	private final boolean shutdownClient;
	private final AbstractRedisClient client;
	private GenericObjectPool<StatefulRedisModulesConnection<String, String>> pool;

	public RedisTimeSeriesMeterRegistry(RedisTimeSeriesConfig config, Clock clock) {
		this(config, clock, config.cluster() ? RedisModulesClusterClient.create(config.uri())
				: RedisModulesClient.create(config.uri()), true, DEFAULT_THREAD_FACTORY);
	}

	public RedisTimeSeriesMeterRegistry(RedisTimeSeriesConfig config, Clock clock, AbstractRedisClient client) {
		this(config, clock, client, false, DEFAULT_THREAD_FACTORY);
	}

	private RedisTimeSeriesMeterRegistry(RedisTimeSeriesConfig config, Clock clock, AbstractRedisClient client,
			boolean shutdownClient, ThreadFactory threadFactory) {
		super(config, clock);
		this.config = config;
		this.client = client;
		this.shutdownClient = shutdownClient;
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

	@Override
	public void start(ThreadFactory threadFactory) {
		if (config.enabled()) {
			pool = ConnectionPoolSupport.createGenericObjectPool(connectionSupplier(client), poolConfig(config));
			super.start(threadFactory);
		}
	}

	@Override
	public void stop() {
		if (pool != null) {
			pool.close();
		}
		if (shutdownClient && client != null) {
			client.shutdown();
			client.getResources().shutdown();
		}
		super.stop();
	}

	private CreateOptions<String, String> createOptions(Meter.Id id, DuplicatePolicy duplicatePolicy) {
		return createOptions(duplicatePolicy, labels(id));
	}

	@SuppressWarnings("unchecked")
	private CreateOptions<String, String> createOptions(DuplicatePolicy duplicatePolicy,
			List<Label<String, String>> labels) {
		return CreateOptions.<String, String>builder().policy(duplicatePolicy).labels(labels.toArray(Label[]::new))
				.build();
	}

	private List<Label<String, String>> labels(Meter.Id id) {
		return labels(getConventionTags(id));
	}

	private List<Label<String, String>> labels(Iterable<Tag> tags) {
		List<Label<String, String>> labels = new ArrayList<>();
		for (Tag tag : tags) {
			labels.add(Label.of(tag.getKey(), tag.getValue()));
		}
		return labels;
	}

	private void createMetersForHistograms(Id id, DistributionStatisticConfig distributionStatisticConfig)
			throws Exception {
		NavigableSet<Double> buckets = distributionStatisticConfig.getHistogramBuckets(false);
		for (Double bucket : buckets) {
			createMeter(vmrange(id, bucket, getBaseTimeUnit()), DuplicatePolicy.LAST, SUFFIX_BUCKET);
		}
	}

	private void createMetersForPercentiles(Id id, DistributionStatisticConfig distributionStatisticConfig)
			throws Exception {
		double[] percentiles = distributionStatisticConfig.getPercentiles();
		if (percentiles == null) {
			return;
		}
		for (double percentile : percentiles) {
			createMeter(quantile(id, percentile), DuplicatePolicy.LAST);
		}
	}

	private void createMeter(Id id, DuplicatePolicy duplicatePolicy, String... suffixes) throws Exception {
		String key = key(id, suffixes);
		try (StatefulRedisModulesConnection<String, String> connection = pool.borrowObject()) {
			RedisModulesCommands<String, String> commands = connection.sync();
			if (commands.exists(key) == 0) {
				commands.tsCreate(key, createOptions(id, duplicatePolicy));
			}
		}
	}

	@Override
	protected Counter newCounter(Id id) {
		try {
			createMeter(id, DuplicatePolicy.SUM);
		} catch (Exception e) {
			log.log(Level.SEVERE, e, () -> "Could not create counter " + id);
		}
		return super.newCounter(id);
	}

	@Override
	protected DistributionSummary newDistributionSummary(Meter.Id id,
			DistributionStatisticConfig distributionStatisticConfig, double scale) {
		try {
			createMeter(id, DuplicatePolicy.SUM, SUFFIX_COUNT);
			createMeter(id, DuplicatePolicy.SUM, SUFFIX_SUM);
			createMeter(id, DuplicatePolicy.MAX, SUFFIX_MAX);
			createMeter(id, DuplicatePolicy.LAST, SUFFIX_MEAN);
			createMetersForPercentiles(id, distributionStatisticConfig);
		} catch (Exception e) {
			log.log(Level.SEVERE, e, () -> "Could not distribution summary " + id);
		}
		return new StepDistributionSummary(id, clock, distributionStatisticConfig, scale, config.step().toMillis(),
				true);
	}

	@Override
	protected <T> FunctionCounter newFunctionCounter(Id id, T obj, ToDoubleFunction<T> countFunction) {
		try {
			createMeter(id, DuplicatePolicy.SUM);
		} catch (Exception e) {
			log.log(Level.SEVERE, e, () -> "Could not create function counter " + id);
		}
		return super.newFunctionCounter(id, obj, countFunction);
	}

	@Override
	protected <T> FunctionTimer newFunctionTimer(Id id, T obj, ToLongFunction<T> countFunction,
			ToDoubleFunction<T> totalTimeFunction, TimeUnit totalTimeFunctionUnit) {
		try {
			createMeter(id, DuplicatePolicy.SUM, SUFFIX_COUNT);
			createMeter(id, DuplicatePolicy.SUM, SUFFIX_SUM);
		} catch (Exception e) {
			log.log(Level.SEVERE, e, () -> "Could not create function timer " + id);
		}
		return super.newFunctionTimer(id, obj, countFunction, totalTimeFunction, totalTimeFunctionUnit);
	}

	@Override
	protected <T> Gauge newGauge(Id id, T obj, ToDoubleFunction<T> valueFunction) {
		try {
			createMeter(id, DuplicatePolicy.LAST);
		} catch (Exception e) {
			log.log(Level.SEVERE, e, () -> "Could not create gauge " + id);
		}
		return super.newGauge(id, obj, valueFunction);
	}

	@Override
	protected LongTaskTimer newLongTaskTimer(Id id, DistributionStatisticConfig distributionStatisticConfig) {
		try {
			createMeter(id, DuplicatePolicy.SUM, SUFFIX_ACTIVE_COUNT);
			createMeter(id, DuplicatePolicy.SUM, SUFFIX_DURATION_SUM);
			createMeter(id, DuplicatePolicy.MAX, SUFFIX_MAX);
			createMetersForPercentiles(id, distributionStatisticConfig);
			createMetersForHistograms(id, distributionStatisticConfig);
		} catch (Exception e) {
			log.log(Level.SEVERE, e, () -> "Could not create long task timer " + id);
		}
		return super.newLongTaskTimer(id, distributionStatisticConfig);
	}

	@Override
	protected <T> TimeGauge newTimeGauge(Id id, T obj, TimeUnit valueFunctionUnit, ToDoubleFunction<T> valueFunction) {
		try {
			createMeter(id, DuplicatePolicy.LAST);
		} catch (Exception e) {
			log.log(Level.SEVERE, e, () -> "Could not create time gauge " + id);
		}
		return super.newTimeGauge(id, obj, valueFunctionUnit, valueFunction);
	}

	@Override
	protected Timer newTimer(Meter.Id id, DistributionStatisticConfig distributionStatisticConfig,
			PauseDetector pauseDetector) {
		try {
			createMeter(id, DuplicatePolicy.SUM, SUFFIX_COUNT);
			createMeter(id, DuplicatePolicy.SUM, SUFFIX_SUM);
			createMeter(id, DuplicatePolicy.LAST, SUFFIX_MEAN);
			createMeter(id, DuplicatePolicy.MAX, SUFFIX_MAX);
			createMetersForPercentiles(id, distributionStatisticConfig);
			createMetersForHistograms(id, distributionStatisticConfig);
		} catch (Exception e) {
			log.log(Level.SEVERE, e, () -> "Could not create timer " + id);
		}
		return new StepTimer(id, clock, distributionStatisticConfig, pauseDetector, getBaseTimeUnit(),
				this.config.step().toMillis(), true);
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
				log.log(Level.SEVERE, "Could not publish measurements", e);
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
			commands.setAutoFlushCommands(false);
			Writer writer = new Writer(commands);
			List<RedisFuture<Long>> futures = batch.stream()
					.flatMap(meter -> meter.match(writer::writeGauge, writer::writeCounter, writer::writeTimer,
							writer::writeDistributionSummary, writer::writeLongTaskTimer, writer::writeTimeGauge,
							writer::writeFunctionCounter, writer::writeFunctionTimer, writer::writeCustomMetric))
					.collect(Collectors.toList());
			commands.flushCommands();
			LettuceFutures.awaitAll(connection.getTimeout().toMillis(), TimeUnit.MILLISECONDS,
					futures.toArray(new Future[0]));
			commands.setAutoFlushCommands(true);
		}
	}

	private class Writer {

		private final RedisTimeSeriesAsyncCommands<String, String> commands;

		public Writer(RedisTimeSeriesAsyncCommands<String, String> commands) {
			this.commands = commands;
		}

		public Stream<RedisFuture<Long>> writeFunctionCounter(FunctionCounter counter) {
			double count = counter.count();
			if (Double.isFinite(count)) {
				return Stream.of(writeMetric(counter.getId(), config().clock().wallTime(), count));
			}
			return Stream.empty();
		}

		public Stream<RedisFuture<Long>> writeCounter(Counter counter) {
			return Stream.of(writeMetric(counter.getId(), config().clock().wallTime(), counter.count()));
		}

		public Stream<RedisFuture<Long>> writeGauge(Gauge gauge) {
			double value = gauge.value();
			if (Double.isFinite(value)) {
				return Stream.of(writeMetric(gauge.getId(), config().clock().wallTime(), value));
			}
			return Stream.empty();
		}

		public Stream<RedisFuture<Long>> writeTimeGauge(TimeGauge timeGauge) {
			double value = timeGauge.value(getBaseTimeUnit());
			if (Double.isFinite(value)) {
				return Stream.of(writeMetric(timeGauge.getId(), config().clock().wallTime(), value));
			}
			return Stream.empty();
		}

		public Stream<RedisFuture<Long>> writeLongTaskTimer(LongTaskTimer timer) {
			List<RedisFuture<Long>> metrics = new ArrayList<>();

			long wallTime = config().clock().wallTime();

			metrics.add(writeMetric(timer.getId(), wallTime, timer.activeTasks(), SUFFIX_ACTIVE_COUNT));
			metrics.add(writeMetric(timer.getId(), wallTime, timer.duration(getBaseTimeUnit()), SUFFIX_DURATION_SUM));
			metrics.add(writeMetric(timer.getId(), wallTime, timer.max(getBaseTimeUnit()), SUFFIX_MAX));

			HistogramSnapshot histogramSnapshot = timer.takeSnapshot();

			final ValueAtPercentile[] percentileValues = histogramSnapshot.percentileValues();
			if (percentileValues.length > 0) {
				metrics.addAll(writePercentiles(timer, wallTime, percentileValues));
			}

			final CountAtBucket[] histogramCounts = histogramSnapshot.histogramCounts();
			if (histogramCounts.length > 0) {
				metrics.addAll(writeHistogram(wallTime, timer, histogramCounts, getBaseTimeUnit()));
			}

			return metrics.stream();
		}

		@SuppressWarnings("unchecked")
		public Stream<RedisFuture<Long>> writeCustomMetric(Meter meter) {
			long wallTime = config().clock().wallTime();
			List<RedisFuture<Long>> futures = new ArrayList<>();
			for (Measurement measurement : meter.measure()) {
				double value = measurement.getValue();
				if (!Double.isFinite(value)) {
					continue;
				}
				String statName = measurement.getStatistic().getTagValueRepresentation();
				List<Label<String, String>> labels = labels(meter.getId());
				labels.add(Label.of("statistic", statName));
				futures.add(commands.tsAdd(key(meter.getId(), statName), Sample.of(wallTime, value),
						AddOptions.<String, String>builder().policy(DuplicatePolicy.LAST)
								.labels(labels.toArray(Label[]::new)).build()));
			}
			return futures.stream();
		}

		private RedisFuture<Long> writeMetric(Meter.Id id, long wallTime, double value, String... suffixes) {
			return commands.tsAdd(key(id, suffixes), Sample.of(wallTime, value));
		}

		private List<RedisFuture<Long>> writeHistogram(long wallTime, Meter meter, CountAtBucket[] histogramCounts,
				TimeUnit timeUnit) {
			List<RedisFuture<Long>> metrics = new ArrayList<>(histogramCounts.length);

			for (CountAtBucket c : histogramCounts) {
				metrics.add(
						writeMetric(vmrange(meter.getId(), c.bucket(), timeUnit), wallTime, c.count(), SUFFIX_BUCKET));
			}

			return metrics;
		}

		public Stream<RedisFuture<Long>> writeDistributionSummary(DistributionSummary summary) {
			long wallTime = config().clock().wallTime();

			final ValueAtPercentile[] percentileValues = summary.takeSnapshot().percentileValues();
			double count = summary.count();

			List<RedisFuture<Long>> metrics = new ArrayList<>();

			metrics.add(writeMetric(summary.getId(), wallTime, count, SUFFIX_COUNT));
			metrics.add(writeMetric(summary.getId(), wallTime, summary.totalAmount(), SUFFIX_SUM));
			metrics.add(writeMetric(summary.getId(), wallTime, summary.max(), SUFFIX_MAX));
			metrics.add(writeMetric(summary.getId(), wallTime, summary.mean(), SUFFIX_MEAN));

			if (percentileValues.length > 0) {
				metrics.addAll(writePercentiles(summary, wallTime, percentileValues));
			}

			return metrics.stream();
		}

		public Stream<RedisFuture<Long>> writeFunctionTimer(FunctionTimer timer) {
			long wallTime = config().clock().wallTime();

			return Stream.of(writeMetric(timer.getId(), wallTime, timer.count(), SUFFIX_COUNT),
					// not applicable
					// writeMetricWithSuffix(timer.getId(), "avg", wallTime,
					// timer.mean(getBaseTimeUnit())),
					writeMetric(timer.getId(), wallTime, timer.totalTime(getBaseTimeUnit()), SUFFIX_SUM));
		}

		public Stream<RedisFuture<Long>> writeTimer(Timer timer) {
			long wallTime = config().clock().wallTime();

			HistogramSnapshot histogramSnapshot = timer.takeSnapshot();
			final ValueAtPercentile[] percentileValues = histogramSnapshot.percentileValues();
			final CountAtBucket[] histogramCounts = histogramSnapshot.histogramCounts();

			List<RedisFuture<Long>> metrics = new ArrayList<>();

			metrics.add(writeMetric(timer.getId(), wallTime, timer.count(), SUFFIX_COUNT));
			metrics.add(writeMetric(timer.getId(), wallTime, timer.totalTime(getBaseTimeUnit()), SUFFIX_SUM));
			metrics.add(writeMetric(timer.getId(), wallTime, timer.mean(getBaseTimeUnit()), SUFFIX_MEAN));
			metrics.add(writeMetric(timer.getId(), wallTime, timer.max(getBaseTimeUnit()), SUFFIX_MAX));

			if (percentileValues.length > 0) {
				metrics.addAll(writePercentiles(timer, wallTime, percentileValues));
			}

			if (histogramCounts.length > 0) {
				metrics.addAll(writeHistogram(wallTime, timer, histogramCounts, getBaseTimeUnit()));
			}

			return metrics.stream();
		}

		private List<RedisFuture<Long>> writePercentiles(Meter meter, long wallTime,
				ValueAtPercentile[] percentileValues) {
			List<RedisFuture<Long>> metrics = new ArrayList<>(percentileValues.length);

			boolean forTimer = meter instanceof Timer;
			for (ValueAtPercentile v : percentileValues) {
				metrics.add(writeMetric(quantile(meter.getId(), v.percentile()), wallTime,
						(forTimer ? v.value(getBaseTimeUnit()) : v.value())));
			}

			return metrics;
		}

	}

	@Override
	protected TimeUnit getBaseTimeUnit() {
		return TimeUnit.MILLISECONDS;
	}

	private Id quantile(Id id, double percentile) {
		return id.withTag(Tag.of("quantile", String.valueOf(percentile)));
	}

	private Id vmrange(Id id, double bucket, TimeUnit timeUnit) {
		String value = getRangeTagValue(timeUnit == null ? bucket : TimeUtils.nanosToUnit(bucket, timeUnit));
		return id.withTag(Tag.of("vmrange", value));
	}

	private String key(Id id, String... suffixes) {
		// usually tagKeys and metricNames naming rules are the same
		// but we can't call getConventionName again after adding suffix
		if (suffixes.length == 0) {
			return prefix(getConventionName(id));
		}
		return prefix(config().namingConvention().tagKey(getConventionName(id) + "." + String.join(".", suffixes)));
	}

	private String prefix(String key) {
		if (config.keyspace() == null) {
			return key;
		}
		return config.keyspace() + RedisTimeSeriesNamingConvention.KEY_SEPARATOR + key;
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

}
