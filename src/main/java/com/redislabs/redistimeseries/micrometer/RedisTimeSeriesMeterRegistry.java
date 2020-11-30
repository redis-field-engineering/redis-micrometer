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
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
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
		this.pool = ConnectionPoolSupport.createGenericObjectPool(client::connect, poolConfig(config));
		start(threadFactory);

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
	public DistributionSummary newDistributionSummary(Meter.Id id,
			DistributionStatisticConfig distributionStatisticConfig, double scale) {
		return new RedisTimeSeriesDistributionSummary(id, clock, distributionStatisticConfig, scale);
	}

	@Override
	protected Timer newTimer(Meter.Id id, DistributionStatisticConfig distributionStatisticConfig,
			PauseDetector pauseDetector) {
		return new RedisTimeSeriesTimer(id, clock, distributionStatisticConfig, pauseDetector);
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
		try (StatefulRedisTimeSeriesConnection<String, String> connection = pool.borrowObject()) {
			RedisTimeSeriesAsyncCommands<String, String> commands = connection.async();
			commands.setAutoFlushCommands(false);
			for (List<Meter> batch : MeterPartition.partition(this, config.batchSize())) {
				List<RedisFuture<Long>> futures = new ArrayList<>(batch.size());
				for (Meter meter : batch) {
					futures.addAll(meter.match(m -> writeGauge(commands, m), m -> writeCounter(commands, m),
							m -> writeTimer(commands, m), m -> writeSummary(commands, m),
							m -> writeLongTaskTimer(commands, m), m -> writeTimeGauge(commands, m),
							m -> writeFunctionCounter(commands, m), m -> writeFunctionTimer(commands, m),
							m -> writeCustomMetric(commands, m)));
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
					} catch (InterruptedException e) {
						return;
					}
				}
			}
			commands.setAutoFlushCommands(true);
		} catch (Exception e) {
			log.error("Could not get connection from pool", e);
		}

	}

	// VisibleForTesting
	List<RedisFuture<Long>> writeGauge(RedisTimeSeriesAsyncCommands<String, String> commands, Gauge gauge) {
		double value = gauge.value();
		if (Double.isFinite(value)) {
			return Arrays.asList(writeMetric(commands, gauge.getId(), config().clock().wallTime(), value));
		}
		return Collections.emptyList();
	}

	List<RedisFuture<Long>> writeSummary(RedisTimeSeriesAsyncCommands<String, String> commands,
			DistributionSummary summary) {
		long wallTime = config().clock().wallTime();

		final ValueAtPercentile[] percentileValues = summary.takeSnapshot().percentileValues();
		final CountAtBucket[] histogramCounts = ((RedisTimeSeriesDistributionSummary) summary).histogramCounts();
		double count = summary.count();

		List<RedisFuture<Long>> metrics = new ArrayList<>();

		metrics.add(writeMetricWithSuffix(commands, summary.getId(), "count", wallTime, count));
		metrics.add(writeMetricWithSuffix(commands, summary.getId(), "sum", wallTime, summary.totalAmount()));
		metrics.add(writeMetricWithSuffix(commands, summary.getId(), "max", wallTime, summary.max()));

		if (percentileValues.length > 0) {
			metrics.addAll(writePercentiles(commands, summary, wallTime, percentileValues));
		}

		if (histogramCounts.length > 0) {
			metrics.addAll(writeHistogram(commands, wallTime, summary, histogramCounts, count, getBaseTimeUnit()));
		}

		return metrics;
	}

	List<RedisFuture<Long>> writeFunctionTimer(RedisTimeSeriesAsyncCommands<String, String> commands,
			FunctionTimer timer) {
		long wallTime = config().clock().wallTime();

		return Arrays.asList(writeMetricWithSuffix(commands, timer.getId(), "count", wallTime, timer.count()),
				// not applicable
				// writeMetricWithSuffix(timer.getId(), "avg", wallTime,
				// timer.mean(getBaseTimeUnit())),
				writeMetricWithSuffix(commands, timer.getId(), "sum", wallTime, timer.totalTime(getBaseTimeUnit())));
	}

	List<RedisFuture<Long>> writeTimer(RedisTimeSeriesAsyncCommands<String, String> commands, Timer timer) {
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

		return metrics;
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
			Meter meter, CountAtBucket[] histogramCounts, double count, @Nullable TimeUnit timeUnit) {
		List<RedisFuture<Long>> metrics = new ArrayList<>(histogramCounts.length);

		// satisfies https://prometheus.io/docs/concepts/metric_types/#histogram, which
		// is at least SOME standard
		// histogram format to follow
		for (CountAtBucket c : histogramCounts) {
			metrics.add(writeMetricWithSuffix(commands,
					meter.getId().withTag(
							new ImmutableTag("le", String.valueOf(timeUnit == null ? c.bucket() : c.bucket(timeUnit)))),
					"bucket", wallTime, c.count()));
		}

		// the +Inf bucket should always equal `count`
		metrics.add(writeMetricWithSuffix(commands, meter.getId().withTag(new ImmutableTag("le", "+Inf")), "bucket",
				wallTime, count));
		return metrics;
	}

	// VisibleForTesting
	List<RedisFuture<Long>> writeFunctionCounter(RedisTimeSeriesAsyncCommands<String, String> commands,
			FunctionCounter counter) {
		double count = counter.count();
		if (Double.isFinite(count)) {
			return Arrays.asList(writeMetric(commands, counter.getId(), config().clock().wallTime(), count));
		}
		return Collections.emptyList();
	}

	List<RedisFuture<Long>> writeCounter(RedisTimeSeriesAsyncCommands<String, String> commands, Counter counter) {
		return Arrays.asList(writeMetric(commands, counter.getId(), config().clock().wallTime(), counter.count()));
	}

	// VisibleForTesting
	List<RedisFuture<Long>> writeTimeGauge(RedisTimeSeriesAsyncCommands<String, String> commands, TimeGauge timeGauge) {
		double value = timeGauge.value(getBaseTimeUnit());
		if (Double.isFinite(value)) {
			return Arrays.asList(writeMetric(commands, timeGauge.getId(), config().clock().wallTime(), value));
		}
		return Collections.emptyList();
	}

	List<RedisFuture<Long>> writeLongTaskTimer(RedisTimeSeriesAsyncCommands<String, String> commands,
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

		return metrics;
	}

	@SuppressWarnings("incomplete-switch")
	private List<RedisFuture<Long>> writeCustomMetric(RedisTimeSeriesAsyncCommands<String, String> commands,
			Meter meter) {
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

	RedisFuture<Long> writeMetric(RedisTimeSeriesAsyncCommands<String, String> commands, Meter.Id id, long wallTime,
			double value) {
		return writeMetricWithSuffix(commands, id, "", wallTime, value);
	}

	RedisFuture<Long> writeMetricWithSuffix(RedisTimeSeriesAsyncCommands<String, String> commands, Meter.Id id,
			String suffix, long wallTime, double value) {
		// usually tagKeys and metricNames naming rules are the same
		// but we can't call getConventionName again after adding suffix
		String key = suffix.isEmpty() ? getConventionName(id)
				: config().namingConvention().tagKey(getConventionName(id) + ":" + suffix);
		return commands.add(key, wallTime, value, labels(id));
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
