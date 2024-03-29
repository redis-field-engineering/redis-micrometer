package com.redis.micrometer;

import static io.micrometer.core.instrument.distribution.FixedBoundaryVictoriaMetricsHistogram.getRangeTagValue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
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
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.step.StepTimer;
import io.micrometer.core.instrument.util.MeterPartition;
import io.micrometer.core.instrument.util.TimeUtils;

/**
 * {@link StepMeterRegistry} for Redis.
 *
 * @author Julien Ruaux
 */
abstract class AbstractRedisMeterRegistry<C extends RedisRegistryConfig> extends StepMeterRegistry {

    public static final String TAG_QUANTILE = "quantile";

    public static final String TAG_VMRANGE = "vmrange";

    private final Logger log = Logger.getLogger(getClass().getName());

    protected final C config;

    private final boolean shutdownClient;

    private final AbstractRedisClient client;

    private final List<RedisFuture<?>> futures = new ArrayList<>();

    private StatefulRedisModulesConnection<String, String> connection;

    protected AbstractRedisMeterRegistry(C config, Clock clock, ThreadFactory threadFactory) {
        this(config, clock, client(config), true, threadFactory);
    }

    protected AbstractRedisMeterRegistry(C config, Clock clock, AbstractRedisClient client, ThreadFactory threadFactory) {
        this(config, clock, client, false, threadFactory);
    }

    private AbstractRedisMeterRegistry(C config, Clock clock, AbstractRedisClient client, boolean shutdownClient,
            ThreadFactory threadFactory) {
        super(config, clock);
        this.config = config;
        this.client = client;
        this.shutdownClient = shutdownClient;
        config().namingConvention(new RedisNamingConvention(config.keySeparator()));
        start(threadFactory);
    }

    private static AbstractRedisClient client(RedisRegistryConfig config) {
        if (config.cluster()) {
            return RedisModulesClusterClient.create(config.uri());
        }
        return RedisModulesClient.create(config.uri());
    }

    @Override
    public void start(ThreadFactory threadFactory) {
        this.connection = RedisModulesUtils.connection(client);
        this.connection.setAutoFlushCommands(false);
        super.start(threadFactory);
    }

    @Override
    public void close() {
        super.close();
        connection.close();
        if (shutdownClient) {
            client.shutdown();
            client.getResources().shutdown();
        }
    }

    protected void addFuture(Function<RedisModulesAsyncCommands<String, String>, RedisFuture<?>> execution) {
        if (!connection.isOpen()) {
            return;
        }
        synchronized (futures) {
            futures.add(execution.apply(connection.async()));
        }
    }

    @Override
    protected void publish() {
        for (List<Meter> batch : MeterPartition.partition(this, config.batchSize())) {
            try {
                write(batch);
            } catch (InterruptedException e) {
                log.log(Level.WARNING, "Interrupted!", e);
                // Restore interrupted state...
                Thread.currentThread().interrupt();
            }
        }
    }

    public void write(Meter... meters) throws InterruptedException {
        write(Arrays.asList(meters));
    }

    public void write(List<Meter> batch) throws InterruptedException {
        if (batch.isEmpty()) {
            return;
        }
        if (!connection.isOpen()) {
            return;
        }
        for (Meter meter : batch) {
            meter.use(this::writeGauge, this::writeCounter, this::writeTimer, this::writeDistributionSummary,
                    this::writeLongTaskTimer, this::writeTimeGauge, this::writeFunctionCounter, this::writeFunctionTimer,
                    this::writeCustomMetric);
        }
        if (!connection.isOpen()) {
            return;
        }
        connection.flushCommands();
        synchronized (futures) {
            try {
                awaitAll();
            } finally {
                futures.clear();
            }
        }
    }

    private void awaitAll() throws InterruptedException {
        if (!connection.isOpen()) {
            return;
        }
        Duration timeout = connection.getTimeout();
        long nanos = timeout.toNanos();
        long time = System.nanoTime();
        try {
            for (RedisFuture<?> f : futures) {
                if (!connection.isOpen()) {
                    return;
                }
                try {
                    if (timeout.isZero() || timeout.isNegative()) {
                        f.get();
                    } else {
                        if (nanos < 0) {
                            return;
                        }
                        f.get(nanos, TimeUnit.NANOSECONDS);
                        long now = System.nanoTime();
                        nanos -= now - time;
                        time = now;
                    }
                } catch (InterruptedException e) {
                    throw e;
                } catch (Exception e) {
                    handleExecutionException(e);
                }
            }
        } catch (TimeoutException e) {
            // ignore
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw Exceptions.fromSynchronization(e);
        }
    }

    protected void handleExecutionException(Exception e) throws Exception {
        throw e;
    }

    protected Stream<Tag> histogramTags(DistributionStatisticConfig distributionStatisticConfig) {
        TimeUnit timeUnit = getBaseTimeUnit();
        NavigableSet<Double> buckets = distributionStatisticConfig.getHistogramBuckets(false);
        return buckets.stream().map(b -> vmrangeTag(b, timeUnit));
    }

    protected Tag vmrangeTag(double bucket, TimeUnit timeUnit) {
        String value = getRangeTagValue(timeUnit == null ? bucket : TimeUtils.nanosToUnit(bucket, timeUnit));
        return Tag.of(TAG_VMRANGE, value);
    }

    protected Stream<Tag> percentileTags(DistributionStatisticConfig distributionStatisticConfig) {
        double[] percentiles = distributionStatisticConfig.getPercentiles();
        if (percentiles == null) {
            return Stream.empty();
        }
        return DoubleStream.of(percentiles).mapToObj(this::quantileTag);
    }

    private Tag quantileTag(double percentile) {
        return Tag.of(TAG_QUANTILE, String.valueOf(percentile));
    }

    @Override
    protected Timer newTimer(Id id, DistributionStatisticConfig distributionStatisticConfig, PauseDetector pauseDetector) {
        return new StepTimer(id, clock, distributionStatisticConfig, pauseDetector, getBaseTimeUnit(),
                this.config.step().toMillis(), true);
    }

    public void writeFunctionCounter(FunctionCounter counter) {
        double count = counter.count();
        if (Double.isFinite(count)) {
            write(counter, count);
        }
    }

    public void writeCounter(Counter counter) {
        write(counter, counter.count());
    }

    public void writeGauge(Gauge gauge) {
        double value = gauge.value();
        if (Double.isFinite(value)) {
            write(gauge, value);
        }
    }

    public void writeTimeGauge(TimeGauge timeGauge) {
        double value = timeGauge.value(getBaseTimeUnit());
        if (Double.isFinite(value)) {
            write(timeGauge, value);
        }
    }

    protected abstract void writeLongTaskTimer(LongTaskTimer timer);

    protected abstract void writeCustomMetric(Meter meter);

    protected abstract void writeDistributionSummary(DistributionSummary summary);

    protected abstract void writeFunctionTimer(FunctionTimer timer);

    protected abstract void writeTimer(Timer timer);

    protected abstract void write(Meter meter, double amount);

    @Override
    protected TimeUnit getBaseTimeUnit() {
        return TimeUnit.MILLISECONDS;
    }

    protected String key(Id id) {
        return prefix(getConventionName(id));
    }

    protected String getConventionName(Id id, Iterable<Tag> tags) {
        StringBuilder name = new StringBuilder();
        name.append(super.getConventionName(id));
        for (Tag tag : tags) {
            name.append(config.keySeparator()).append(tag.getValue());
        }
        return name.toString();
    }

    protected String key(Id id, String suffix) {
        // usually tagKeys and metricNames naming rules are the same
        // but we can't call getConventionName again after adding suffix
        return prefix(config().namingConvention().tagKey(getConventionName(id) + "." + suffix));
    }

    protected String prefix(String key) {
        if (config.keyspace() == null) {
            return key;
        }
        return config.keyspace() + config.keySeparator() + key;
    }

    @Override
    protected String getConventionName(Id id) {
        return getConventionName(id, getConventionTags(id));
    }

    protected Map<Id, Double> percentileValues(Meter meter, HistogramSnapshot histogram) {
        Map<Id, Double> map = new HashMap<>();
        ToDoubleFunction<ValueAtPercentile> doubleFunction = percentileToDoubleFunction(meter);
        for (ValueAtPercentile value : histogram.percentileValues()) {
            map.put(meter.getId().withTag(quantileTag(value.percentile())), doubleFunction.applyAsDouble(value));
        }
        return map;
    }

    private ToDoubleFunction<ValueAtPercentile> percentileToDoubleFunction(Meter meter) {
        if (meter instanceof Timer) {
            return v -> v.value(getBaseTimeUnit());
        }
        return ValueAtPercentile::value;
    }

    protected Map<Id, Double> histogramCounts(Meter meter, HistogramSnapshot histogram) {
        Map<Id, Double> map = new HashMap<>();
        TimeUnit timeUnit = getBaseTimeUnit();
        for (CountAtBucket c : histogram.histogramCounts()) {
            map.put(meter.getId().withTag(vmrangeTag(c.bucket(), timeUnit)), c.count());
        }
        return map;
    }

    protected Map<String, Double> statistics(Meter meter) {
        Map<String, Double> stats = new HashMap<>();
        for (Measurement measurement : meter.measure()) {
            double value = measurement.getValue();
            if (!Double.isFinite(value)) {
                continue;
            }
            stats.put(measurement.getStatistic().getTagValueRepresentation(), value);
        }
        return stats;
    }

}
