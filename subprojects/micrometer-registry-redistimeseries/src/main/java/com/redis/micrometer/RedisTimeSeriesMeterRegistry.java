package com.redis.micrometer;

import static io.micrometer.core.instrument.distribution.FixedBoundaryVictoriaMetricsHistogram.getRangeTagValue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;
import java.util.logging.Logger;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.Label;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.util.RedisModulesUtils;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.internal.Exceptions;
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

	private static final String ERROR_KEY_ALREADY_EXISTS = "ERR TSDB: key already exists";

	private final Logger log = Logger.getLogger(RedisTimeSeriesMeterRegistry.class.getName());

	private final RedisTimeSeriesConfig config;
	private final boolean shutdownClient;
	private final AbstractRedisClient client;
	private final StatefulRedisModulesConnection<String, String> connection;
	private final RedisModulesAsyncCommands<String, String> commands;
	private final List<RedisFuture<?>> futures = new ArrayList<>();

	public RedisTimeSeriesMeterRegistry(RedisTimeSeriesConfig config, Clock clock) {
		this(config, clock, client(config), true, DEFAULT_THREAD_FACTORY);
	}

	private static AbstractRedisClient client(RedisTimeSeriesConfig config) {
		if (config.cluster()) {
			return RedisModulesClusterClient.create(config.uri());
		}
		return RedisModulesClient.create(config.uri());
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
		this.connection = RedisModulesUtils.connection(client);
		this.connection.setAutoFlushCommands(false);
		this.commands = connection.async();
		config().namingConvention(new RedisTimeSeriesNamingConvention());
		start(threadFactory);
	}

	@Override
	public void stop() {
		connection.close();
		if (shutdownClient) {
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

	private void createMetersForHistograms(Id id, DistributionStatisticConfig distributionStatisticConfig) {
		NavigableSet<Double> buckets = distributionStatisticConfig.getHistogramBuckets(false);
		for (Double bucket : buckets) {
			createMeter(vmrange(id, bucket, getBaseTimeUnit()), DuplicatePolicy.LAST, SUFFIX_BUCKET);
		}
	}

	private void createMetersForPercentiles(Id id, DistributionStatisticConfig distributionStatisticConfig) {
		double[] percentiles = distributionStatisticConfig.getPercentiles();
		if (percentiles == null) {
			return;
		}
		for (double percentile : percentiles) {
			createMeter(quantile(id, percentile), DuplicatePolicy.LAST);
		}
	}

	private void createMeter(Id id, DuplicatePolicy duplicatePolicy, String... suffixes) {
		futures.add(commands.tsCreate(key(id, suffixes), createOptions(id, duplicatePolicy)));
	}

	@Override
	protected Counter newCounter(Id id) {
		createMeter(id, DuplicatePolicy.SUM);
		return super.newCounter(id);
	}

	@Override
	protected DistributionSummary newDistributionSummary(Meter.Id id,
			DistributionStatisticConfig distributionStatisticConfig, double scale) {
		createMeter(id, DuplicatePolicy.SUM, SUFFIX_COUNT);
		createMeter(id, DuplicatePolicy.SUM, SUFFIX_SUM);
		createMeter(id, DuplicatePolicy.MAX, SUFFIX_MAX);
		createMeter(id, DuplicatePolicy.LAST, SUFFIX_MEAN);
		createMetersForPercentiles(id, distributionStatisticConfig);
		return new StepDistributionSummary(id, clock, distributionStatisticConfig, scale, config.step().toMillis(),
				true);
	}

	@Override
	protected <T> FunctionCounter newFunctionCounter(Id id, T obj, ToDoubleFunction<T> countFunction) {
		createMeter(id, DuplicatePolicy.SUM);
		return super.newFunctionCounter(id, obj, countFunction);
	}

	@Override
	protected <T> FunctionTimer newFunctionTimer(Id id, T obj, ToLongFunction<T> countFunction,
			ToDoubleFunction<T> totalTimeFunction, TimeUnit totalTimeFunctionUnit) {
		createMeter(id, DuplicatePolicy.SUM, SUFFIX_COUNT);
		createMeter(id, DuplicatePolicy.SUM, SUFFIX_SUM);
		return super.newFunctionTimer(id, obj, countFunction, totalTimeFunction, totalTimeFunctionUnit);
	}

	@Override
	protected <T> Gauge newGauge(Id id, T obj, ToDoubleFunction<T> valueFunction) {
		createMeter(id, DuplicatePolicy.LAST);
		return super.newGauge(id, obj, valueFunction);
	}

	@Override
	protected LongTaskTimer newLongTaskTimer(Id id, DistributionStatisticConfig distributionStatisticConfig) {
		createMeter(id, DuplicatePolicy.SUM, SUFFIX_ACTIVE_COUNT);
		createMeter(id, DuplicatePolicy.SUM, SUFFIX_DURATION_SUM);
		createMeter(id, DuplicatePolicy.MAX, SUFFIX_MAX);
		createMetersForPercentiles(id, distributionStatisticConfig);
		createMetersForHistograms(id, distributionStatisticConfig);
		return super.newLongTaskTimer(id, distributionStatisticConfig);
	}

	@Override
	protected <T> TimeGauge newTimeGauge(Id id, T obj, TimeUnit valueFunctionUnit, ToDoubleFunction<T> valueFunction) {
		createMeter(id, DuplicatePolicy.LAST);
		return super.newTimeGauge(id, obj, valueFunctionUnit, valueFunction);
	}

	@Override
	protected Timer newTimer(Meter.Id id, DistributionStatisticConfig distributionStatisticConfig,
			PauseDetector pauseDetector) {
		createMeter(id, DuplicatePolicy.SUM, SUFFIX_COUNT);
		createMeter(id, DuplicatePolicy.SUM, SUFFIX_SUM);
		createMeter(id, DuplicatePolicy.LAST, SUFFIX_MEAN);
		createMeter(id, DuplicatePolicy.MAX, SUFFIX_MAX);
		createMetersForPercentiles(id, distributionStatisticConfig);
		createMetersForHistograms(id, distributionStatisticConfig);
		return new StepTimer(id, clock, distributionStatisticConfig, pauseDetector, getBaseTimeUnit(),
				this.config.step().toMillis(), true);
	}

	@Override
	protected void publish() {
		if (client == null) {
			log.info("Client is null, skipping publish");
			return;
		}
		MeterPartition.partition(this, config.batchSize()).forEach(this::write);
	}

	public void write(Meter... meters) {
		write(Arrays.asList(meters));
	}

	public boolean write(List<Meter> batch) {
		if (batch.isEmpty()) {
			return true;
		}
		for (Meter meter : batch) {
			meter.use(this::writeGauge, this::writeCounter, this::writeTimer, this::writeDistributionSummary,
					this::writeLongTaskTimer, this::writeTimeGauge, this::writeFunctionCounter,
					this::writeFunctionTimer, this::writeCustomMetric);
		}
		connection.flushCommands();
		try {
			return awaitAll();
		} finally {
			futures.clear();
		}
	}

	private boolean awaitAll() {
		Duration timeout = connection.getTimeout();
		long nanos = timeout.toNanos();
		long time = System.nanoTime();
		try {
			for (RedisFuture<?> f : futures) {
				try {
					if (timeout.isZero() || timeout.isNegative()) {
						f.get();
					} else {
						if (nanos < 0) {
							return false;
						}
						f.get(nanos, TimeUnit.NANOSECONDS);
						long now = System.nanoTime();
						nanos -= now - time;
						time = now;
					}
				} catch (ExecutionException e) {
					if (e.getCause() != null && ERROR_KEY_ALREADY_EXISTS.equals(e.getCause().getMessage())) {
						continue;
					}
					throw e;
				}
			}
			return true;
		} catch (TimeoutException e) {
			return false;
		} catch (Exception e) {
			throw Exceptions.fromSynchronization(e);
		}
	}

	public void writeFunctionCounter(FunctionCounter counter) {
		double count = counter.count();
		if (Double.isFinite(count)) {
			writeMetric(counter.getId(), config().clock().wallTime(), count);
		}
	}

	public void writeCounter(Counter counter) {
		writeMetric(counter.getId(), config().clock().wallTime(), counter.count());
	}

	public void writeGauge(Gauge gauge) {
		double value = gauge.value();
		if (Double.isFinite(value)) {
			writeMetric(gauge.getId(), config().clock().wallTime(), value);
		}
	}

	public void writeTimeGauge(TimeGauge timeGauge) {
		double value = timeGauge.value(getBaseTimeUnit());
		if (Double.isFinite(value)) {
			writeMetric(timeGauge.getId(), config().clock().wallTime(), value);
		}
	}

	public void writeLongTaskTimer(LongTaskTimer timer) {
		long wallTime = config().clock().wallTime();

		writeMetric(timer.getId(), wallTime, timer.activeTasks(), SUFFIX_ACTIVE_COUNT);
		writeMetric(timer.getId(), wallTime, timer.duration(getBaseTimeUnit()), SUFFIX_DURATION_SUM);
		writeMetric(timer.getId(), wallTime, timer.max(getBaseTimeUnit()), SUFFIX_MAX);

		HistogramSnapshot histogramSnapshot = timer.takeSnapshot();

		final ValueAtPercentile[] percentileValues = histogramSnapshot.percentileValues();
		if (percentileValues.length > 0) {
			writePercentiles(timer, wallTime, percentileValues);
		}

		final CountAtBucket[] histogramCounts = histogramSnapshot.histogramCounts();
		if (histogramCounts.length > 0) {
			writeHistogram(wallTime, timer, histogramCounts, getBaseTimeUnit());
		}
	}

	@SuppressWarnings("unchecked")
	public void writeCustomMetric(Meter meter) {
		long wallTime = config().clock().wallTime();
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
	}

	private void writeMetric(Meter.Id id, long wallTime, double value, String... suffixes) {
		futures.add(commands.tsAdd(key(id, suffixes), Sample.of(wallTime, value)));
	}

	private void writeHistogram(long wallTime, Meter meter, CountAtBucket[] histogramCounts, TimeUnit timeUnit) {
		for (CountAtBucket c : histogramCounts) {
			writeMetric(vmrange(meter.getId(), c.bucket(), timeUnit), wallTime, c.count(), SUFFIX_BUCKET);
		}
	}

	public void writeDistributionSummary(DistributionSummary summary) {
		long wallTime = config().clock().wallTime();

		final ValueAtPercentile[] percentileValues = summary.takeSnapshot().percentileValues();
		double count = summary.count();

		writeMetric(summary.getId(), wallTime, count, SUFFIX_COUNT);
		writeMetric(summary.getId(), wallTime, summary.totalAmount(), SUFFIX_SUM);
		writeMetric(summary.getId(), wallTime, summary.max(), SUFFIX_MAX);
		writeMetric(summary.getId(), wallTime, summary.mean(), SUFFIX_MEAN);

		if (percentileValues.length > 0) {
			writePercentiles(summary, wallTime, percentileValues);
		}

	}

	public void writeFunctionTimer(FunctionTimer timer) {
		long wallTime = config().clock().wallTime();
		writeMetric(timer.getId(), wallTime, timer.count(), SUFFIX_COUNT);
		writeMetric(timer.getId(), wallTime, timer.totalTime(getBaseTimeUnit()), SUFFIX_SUM);
	}

	public void writeTimer(Timer timer) {
		long wallTime = config().clock().wallTime();

		HistogramSnapshot histogramSnapshot = timer.takeSnapshot();
		final ValueAtPercentile[] percentileValues = histogramSnapshot.percentileValues();
		final CountAtBucket[] histogramCounts = histogramSnapshot.histogramCounts();

		writeMetric(timer.getId(), wallTime, timer.count(), SUFFIX_COUNT);
		writeMetric(timer.getId(), wallTime, timer.totalTime(getBaseTimeUnit()), SUFFIX_SUM);
		writeMetric(timer.getId(), wallTime, timer.mean(getBaseTimeUnit()), SUFFIX_MEAN);
		writeMetric(timer.getId(), wallTime, timer.max(getBaseTimeUnit()), SUFFIX_MAX);

		if (percentileValues.length > 0) {
			writePercentiles(timer, wallTime, percentileValues);
		}

		if (histogramCounts.length > 0) {
			writeHistogram(wallTime, timer, histogramCounts, getBaseTimeUnit());
		}
	}

	private void writePercentiles(Meter meter, long wallTime, ValueAtPercentile[] percentileValues) {
		boolean forTimer = meter instanceof Timer;
		for (ValueAtPercentile v : percentileValues) {
			writeMetric(quantile(meter.getId(), v.percentile()), wallTime,
					(forTimer ? v.value(getBaseTimeUnit()) : v.value()));
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
