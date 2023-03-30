package com.redis.micrometer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.Field;

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
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.util.NamedThreadFactory;

/**
 * {@link StepMeterRegistry} for RedisTimeSeries.
 *
 * @author Julien Ruaux
 */
public class RediSearchMeterRegistry extends AbstractRedisMeterRegistry<RediSearchRegistryConfig> {

	private static final ThreadFactory DEFAULT_THREAD_FACTORY = new NamedThreadFactory("redisearch-metrics-publisher");
	private static final NamingConvention FIELD_NAMING_CONVENTION = NamingConvention.camelCase;

	public static final String FIELD_TIME = "time";
	public static final String FIELD_COUNT = "count";
	public static final String FIELD_SUM = "sum";
	public static final String FIELD_MAX = "max";
	public static final String FIELD_MEAN = "mean";
	public static final String FIELD_DURATION_SUM = "durationSum";
	public static final String FIELD_ACTIVE_COUNT = "activeCount";
	public static final String FIELD_VALUE = "value";

	private static final String ERROR_INDEX_ALREADY_EXISTS = "Index already exists";

	private final Function<Id, String> indexNamingFunction;
	private final Set<String> excludedKeyTags;

	public RediSearchMeterRegistry(RediSearchRegistryConfig config, Clock clock) {
		super(config, clock, DEFAULT_THREAD_FACTORY);
		indexNamingFunction = indexNamingFunction(config);
		excludedKeyTags = excludedKeyTags(config);
	}

	public RediSearchMeterRegistry(RediSearchRegistryConfig config, Clock clock, AbstractRedisClient client) {
		super(config, clock, client, DEFAULT_THREAD_FACTORY);
		indexNamingFunction = indexNamingFunction(config);
		excludedKeyTags = excludedKeyTags(config);
	}

	private Function<Id, String> indexNamingFunction(RediSearchRegistryConfig config) {
		NamingConvention namingConvention = new RedisNamingConvention(config.indexSeparator());
		Function<Id, String> idFunction = id -> id.getConventionName(namingConvention);
		if (config.indexPrefix() == null) {
			if (config.indexSuffix() == null) {
				return idFunction;
			}
			String suffix = suffix(config);
			return id -> idFunction.apply(id) + suffix;
		}
		String prefix = config.indexPrefix() + config.indexSeparator();
		if (config.indexSuffix() == null) {
			return id -> prefix + idFunction.apply(id);
		}
		String suffix = suffix(config);
		return id -> prefix + idFunction.apply(id) + suffix;
	}

	private String suffix(RediSearchRegistryConfig config) {
		return config.indexSeparator() + config.indexSuffix();
	}

	private Set<String> excludedKeyTags(RediSearchRegistryConfig config) {
		return Stream.of(config.nonKeyTags()).collect(Collectors.toSet());
	}

	@Override
	protected String getConventionName(Id id) {
		if (config.nonKeyTags().length == 0) {
			return super.getConventionName(id);
		}
		List<Tag> tags = getConventionTags(id).stream().filter(t -> !excludedKeyTags.contains(t.getKey()))
				.collect(Collectors.toList());
		return getConventionName(id, tags);
	}

	@Override
	protected void handleExecutionException(Exception e) throws Exception {
		if (e.getCause() != null && ERROR_INDEX_ALREADY_EXISTS.equals(e.getCause().getMessage())) {
			return;
		}
		super.handleExecutionException(e);
	}

	private String index(Id id) {
		return indexNamingFunction.apply(id);
	}

	@Override
	protected Counter newCounter(Id id) {
		createMeter(id, FIELD_VALUE);
		return super.newCounter(id);
	}

	@Override
	protected DistributionSummary newDistributionSummary(Id id, DistributionStatisticConfig distributionStatisticConfig,
			double scale) {
		List<String> fields = new ArrayList<>();
		fields.addAll(Arrays.asList(FIELD_COUNT, FIELD_SUM, FIELD_MAX, FIELD_MEAN));
		fields.addAll(toFieldNames(percentileTags(distributionStatisticConfig)));
		createMeter(id, fields);
		return super.newDistributionSummary(id, distributionStatisticConfig, scale);
	}

	@Override
	protected <T> FunctionCounter newFunctionCounter(Id id, T obj, ToDoubleFunction<T> countFunction) {
		createMeter(id, FIELD_VALUE);
		return super.newFunctionCounter(id, obj, countFunction);
	}

	@Override
	protected <T> FunctionTimer newFunctionTimer(Id id, T obj, ToLongFunction<T> countFunction,
			ToDoubleFunction<T> totalTimeFunction, TimeUnit totalTimeFunctionUnit) {
		createMeter(id, FIELD_COUNT, FIELD_SUM);
		return super.newFunctionTimer(id, obj, countFunction, totalTimeFunction, totalTimeFunctionUnit);
	}

	@Override
	protected <T> Gauge newGauge(Id id, T obj, ToDoubleFunction<T> valueFunction) {
		createMeter(id, FIELD_VALUE);
		return super.newGauge(id, obj, valueFunction);
	}

	@Override
	protected <T> TimeGauge newTimeGauge(Id id, T obj, TimeUnit valueFunctionUnit, ToDoubleFunction<T> valueFunction) {
		createMeter(id, FIELD_VALUE);
		return super.newTimeGauge(id, obj, valueFunctionUnit, valueFunction);
	}

	@Override
	protected Timer newTimer(Id id, DistributionStatisticConfig distributionStatisticConfig,
			PauseDetector pauseDetector) {
		List<String> fields = new ArrayList<>();
		fields.addAll(Arrays.asList(FIELD_COUNT, FIELD_SUM, FIELD_MEAN, FIELD_MAX));
		fields.addAll(fieldNames(distributionStatisticConfig));
		createMeter(id, fields);
		return super.newTimer(id, distributionStatisticConfig, pauseDetector);
	}

	@Override
	protected LongTaskTimer newLongTaskTimer(Id id, DistributionStatisticConfig distributionStatisticConfig) {
		List<String> fields = new ArrayList<>();
		fields.addAll(Arrays.asList(FIELD_ACTIVE_COUNT, FIELD_DURATION_SUM, FIELD_MAX));
		fields.addAll(fieldNames(distributionStatisticConfig));
		createMeter(id, fields);
		return super.newLongTaskTimer(id, distributionStatisticConfig);
	}

	private List<String> fieldNames(DistributionStatisticConfig distributionStatisticConfig) {
		Stream<Tag> percentileTags = percentileTags(distributionStatisticConfig);
		Stream<Tag> histogramTags = histogramTags(distributionStatisticConfig);
		return toFieldNames(Stream.concat(percentileTags, histogramTags));
	}

	private List<String> toFieldNames(Stream<Tag> tags) {
		return tags.map(t -> FIELD_NAMING_CONVENTION.tagKey(t.getKey() + "." + t.getValue()))
				.collect(Collectors.toList());
	}

	private void createMeter(Id id, String... fields) {
		createMeter(id, Arrays.asList(fields));
	}

	@SuppressWarnings("unchecked")
	private void createMeter(Id id, Iterable<String> numericFields) {
		String index = index(id);
		List<Field<String>> fields = new ArrayList<>();
		fields.add(Field.numeric(FIELD_TIME).sortable().build());
		for (String field : numericFields) {
			fields.add(Field.numeric(field).sortable().build());
		}
		fields.addAll(getConventionTags(id).stream().map(t -> Field.tag(t.getKey()).sortable().withSuffixTrie().build())
				.collect(Collectors.toList()));
		CreateOptions.Builder<String, String> options = CreateOptions.builder();
		options.prefix(prefix(id.getConventionName(config().namingConvention())));
		addFuture(c -> c.ftCreate(index, options.build(), fields.toArray(new Field[0])));

	}

	private static class MeterDocument extends Document<String, String> {

		private static final long serialVersionUID = 1L;

		public void setTime(long wallTime) {
			super.put(FIELD_TIME, String.valueOf(wallTime));
		}

		public void put(String field, double value) {
			super.put(field, String.valueOf(value));
		}

		public void put(Id id, double value) {
			put(id.getConventionName(FIELD_NAMING_CONVENTION), value);
		}

		public void putValues(Map<Id, Double> values) {
			values.forEach(this::put);
		}

	}

	private MeterDocument doc(Meter meter) {
		MeterDocument doc = new MeterDocument();
		doc.setId(key(meter.getId()));
		doc.setTime(config().clock().wallTime());
		getConventionTags(meter.getId()).forEach(t -> doc.put(t.getKey(), t.getValue()));
		return doc;
	}

	@Override
	public void writeTimer(Timer timer) {
		MeterDocument doc = doc(timer);
		doc.put(FIELD_MEAN, timer.mean(getBaseTimeUnit()));
		doc.put(FIELD_MAX, timer.max(getBaseTimeUnit()));
		HistogramSnapshot histogram = timer.takeSnapshot();
		doc.putValues(percentileValues(timer, histogram));
		doc.putValues(histogramCounts(timer, histogram));
		write(doc);
		hincrby(doc, FIELD_COUNT, timer.count());
		hincrbyfloat(doc, FIELD_SUM, timer.totalTime(getBaseTimeUnit()));
	}

	@Override
	protected void writeLongTaskTimer(LongTaskTimer timer) {
		MeterDocument doc = doc(timer);
		doc.put(FIELD_MAX, timer.max(getBaseTimeUnit()));
		doc.put(FIELD_ACTIVE_COUNT, timer.activeTasks());
		HistogramSnapshot histogram = timer.takeSnapshot();
		doc.putValues(percentileValues(timer, histogram));
		doc.putValues(histogramCounts(timer, histogram));
		write(doc);
		hincrbyfloat(doc, FIELD_DURATION_SUM, timer.duration(getBaseTimeUnit()));
	}

	private void hincrbyfloat(MeterDocument doc, String field, double amount) {
		addFuture(c -> c.hincrbyfloat(doc.getId(), field, amount));
	}

	private void hincrby(MeterDocument doc, String field, long amount) {
		addFuture(c -> c.hincrby(doc.getId(), field, amount));
	}

	private void write(MeterDocument doc) {
		addFuture(c -> c.hset(doc.getId(), doc));
	}

	@Override
	protected void write(Meter meter, double amount) {
		MeterDocument doc = doc(meter);
		write(doc);
		hincrbyfloat(doc, FIELD_VALUE, amount);
	}

	@Override
	protected void writeCustomMetric(Meter meter) {
		MeterDocument doc = doc(meter);
		statistics(meter).forEach(doc::put);
		write(doc);
	}

	@Override
	protected void writeDistributionSummary(DistributionSummary summary) {
		MeterDocument doc = doc(summary);
		doc.put(FIELD_SUM, summary.totalAmount());
		doc.put(FIELD_MAX, summary.max());
		doc.put(FIELD_MEAN, summary.mean());
		doc.putValues(percentileValues(summary, summary.takeSnapshot()));
		write(doc);
		hincrby(doc, FIELD_COUNT, summary.count());
	}

	@Override
	protected void writeFunctionTimer(FunctionTimer timer) {
		MeterDocument doc = doc(timer);
		write(doc);
		hincrbyfloat(doc, FIELD_COUNT, timer.count());
		hincrbyfloat(doc, FIELD_SUM, timer.totalTime(getBaseTimeUnit()));
	}

}
