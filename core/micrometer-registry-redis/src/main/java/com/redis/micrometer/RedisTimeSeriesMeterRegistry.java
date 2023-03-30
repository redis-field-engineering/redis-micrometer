package com.redis.micrometer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.Label;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.AbstractRedisClient;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.step.StepDistributionSummary;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.util.NamedThreadFactory;

/**
 * {@link StepMeterRegistry} for RedisTimeSeries.
 *
 * @author Julien Ruaux
 */
public class RedisTimeSeriesMeterRegistry extends AbstractRedisMeterRegistry<RedisRegistryConfig> {

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

	private static final String TAG_STAT = "stat";
	private static final String TAG_NAME = "name";

	public RedisTimeSeriesMeterRegistry(RedisRegistryConfig config, Clock clock) {
		super(config, clock, DEFAULT_THREAD_FACTORY);
	}

	public RedisTimeSeriesMeterRegistry(RedisRegistryConfig config, Clock clock, AbstractRedisClient client) {
		super(config, clock, client, DEFAULT_THREAD_FACTORY);
	}

	@Override
	protected void handleExecutionException(Exception e) throws Exception {
		if (e.getCause() != null && ERROR_KEY_ALREADY_EXISTS.equals(e.getCause().getMessage())) {
			return;
		}
		super.handleExecutionException(e);
	}

	private void createMeter(Id id, DuplicatePolicy duplicatePolicy, String suffix) {
		createMeter(key(id, suffix), duplicatePolicy, tags(id, suffix));
	}

	private List<Tag> tags(Id id) {
		List<Tag> tags = new ArrayList<>();
		tags.add(Tag.of(TAG_NAME, id.getName()));
		tags.addAll(getConventionTags(id));
		return tags;
	}

	private List<Tag> tags(Id id, String suffix) {
		List<Tag> tags = tags(id);
		tags.add(Tag.of(TAG_STAT, suffix));
		return tags;
	}

	private void createMeter(Id id, DuplicatePolicy duplicatePolicy) {
		createMeter(key(id), duplicatePolicy, tags(id));
	}

	@SuppressWarnings("unchecked")
	private void createMeter(String key, DuplicatePolicy duplicatePolicy, Iterable<Tag> tags) {
		CreateOptions<String, String> options = CreateOptions.<String, String>builder().policy(duplicatePolicy)
				.labels(labels(tags).toArray(new Label[0])).build();
		addFuture(c -> c.tsCreate(key, options));
	}

	private List<Label<String, String>> labels(Iterable<Tag> tags) {
		List<Label<String, String>> labels = new ArrayList<>();
		for (Tag tag : tags) {
			labels.add(Label.of(tag.getKey(), tag.getValue()));
		}
		return labels;
	}

	@Override
	protected Counter newCounter(Id id) {
		createMeter(id, DuplicatePolicy.SUM);
		return super.newCounter(id);
	}

	@Override
	protected DistributionSummary newDistributionSummary(Id id, DistributionStatisticConfig distributionStatisticConfig,
			double scale) {
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

	private void createMetersForHistograms(Id id, DistributionStatisticConfig distributionStatisticConfig) {
		histogramTags(distributionStatisticConfig).forEach(t -> createMeter(id.withTag(t), DuplicatePolicy.LAST));
	}

	private void createMetersForPercentiles(Id id, DistributionStatisticConfig distributionStatisticConfig) {
		percentileTags(distributionStatisticConfig).forEach(t -> createMeter(id.withTag(t), DuplicatePolicy.LAST));
	}

	protected Id quantile(Id id, double percentile) {
		return id.withTag(Tag.of("quantile", String.valueOf(percentile)));
	}

	@Override
	protected <T> TimeGauge newTimeGauge(Id id, T obj, TimeUnit valueFunctionUnit, ToDoubleFunction<T> valueFunction) {
		createMeter(id, DuplicatePolicy.LAST);
		return super.newTimeGauge(id, obj, valueFunctionUnit, valueFunction);
	}

	@Override
	protected Timer newTimer(Id id, DistributionStatisticConfig distributionStatisticConfig,
			PauseDetector pauseDetector) {
		createMeter(id, DuplicatePolicy.SUM, SUFFIX_COUNT);
		createMeter(id, DuplicatePolicy.SUM, SUFFIX_SUM);
		createMeter(id, DuplicatePolicy.LAST, SUFFIX_MEAN);
		createMeter(id, DuplicatePolicy.MAX, SUFFIX_MAX);
		createMetersForPercentiles(id, distributionStatisticConfig);
		createMetersForHistograms(id, distributionStatisticConfig);
		return super.newTimer(id, distributionStatisticConfig, pauseDetector);
	}

	@Override
	public void writeLongTaskTimer(LongTaskTimer timer) {
		long wallTime = config().clock().wallTime();
		write(timer, wallTime, timer.activeTasks(), SUFFIX_ACTIVE_COUNT);
		write(timer, wallTime, timer.duration(getBaseTimeUnit()), SUFFIX_DURATION_SUM);
		write(timer, wallTime, timer.max(getBaseTimeUnit()), SUFFIX_MAX);

		HistogramSnapshot histogram = timer.takeSnapshot();
		writePercentiles(timer, wallTime, histogram);
		writeHistogram(timer, wallTime, histogram);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void writeCustomMetric(Meter meter) {
		long wallTime = config().clock().wallTime();
		for (Entry<String, Double> stat : statistics(meter).entrySet()) {
			String statName = stat.getKey();
			String key = key(meter.getId(), statName);
			List<Label<String, String>> labels = new ArrayList<>(labels(tags(meter.getId())));
			labels.add(Label.of("statistic", statName));
			Sample sample = Sample.of(wallTime, stat.getValue());
			AddOptions.Builder<String, String> options = AddOptions.builder();
			options.policy(DuplicatePolicy.LAST);
			options.labels(labels.toArray(new Label[0]));
			addFuture(c -> c.tsAdd(key, sample, options.build()));
		}
	}

	private void write(Id id, long wallTime, double value, String suffix) {
		write(key(id, suffix), wallTime, value);
	}

	private void write(Meter meter, long wallTime, double value, String suffix) {
		write(meter.getId(), wallTime, value, suffix);
	}

	private void write(String key, long time, double value) {
		addFuture(c -> c.tsAdd(key, Sample.of(time, value)));
	}

	private void write(Id id, long time, double value) {
		write(key(id), time, value);
	}

	@Override
	protected void write(Meter meter, double value) {
		write(meter.getId(), config().clock().wallTime(), value);
	}

	private void writeHistogram(Meter meter, long wallTime, HistogramSnapshot histogram) {
		histogramCounts(meter, histogram).forEach((id, v) -> write(id, wallTime, v, SUFFIX_BUCKET));
	}

	@Override
	public void writeDistributionSummary(DistributionSummary summary) {
		long wallTime = config().clock().wallTime();
		write(summary, wallTime, summary.count(), SUFFIX_COUNT);
		write(summary, wallTime, summary.totalAmount(), SUFFIX_SUM);
		write(summary, wallTime, summary.max(), SUFFIX_MAX);
		write(summary, wallTime, summary.mean(), SUFFIX_MEAN);
		writePercentiles(summary, wallTime, summary.takeSnapshot());
	}

	@Override
	public void writeFunctionTimer(FunctionTimer timer) {
		long wallTime = config().clock().wallTime();
		write(timer, wallTime, timer.count(), SUFFIX_COUNT);
		write(timer, wallTime, timer.totalTime(getBaseTimeUnit()), SUFFIX_SUM);
	}

	@Override
	public void writeTimer(Timer timer) {
		long wallTime = config().clock().wallTime();

		write(timer, wallTime, timer.count(), SUFFIX_COUNT);
		write(timer, wallTime, timer.totalTime(getBaseTimeUnit()), SUFFIX_SUM);
		write(timer, wallTime, timer.mean(getBaseTimeUnit()), SUFFIX_MEAN);
		write(timer, wallTime, timer.max(getBaseTimeUnit()), SUFFIX_MAX);

		HistogramSnapshot histogram = timer.takeSnapshot();
		writePercentiles(timer, wallTime, histogram);
		writeHistogram(timer, wallTime, histogram);
	}

	private void writePercentiles(Meter meter, long wallTime, HistogramSnapshot histogram) {
		percentileValues(meter, histogram).forEach((id, value) -> write(id, wallTime, value));
	}

}
