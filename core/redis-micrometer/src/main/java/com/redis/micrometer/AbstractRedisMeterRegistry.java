package com.redis.micrometer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

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
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.util.MeterPartition;

/**
 * {@link StepMeterRegistry} for Redis.
 *
 * @author Julien Ruaux
 */
abstract class AbstractRedisMeterRegistry extends StepMeterRegistry {

	private final Logger log = Logger.getLogger(getClass().getName());

	protected final RedisConfig config;
	private final boolean shutdownClient;
	private final AbstractRedisClient client;
	private final StatefulRedisModulesConnection<String, String> connection;
	private final List<RedisFuture<?>> futures = new ArrayList<>();

	protected AbstractRedisMeterRegistry(RedisConfig config, Clock clock, ThreadFactory threadFactory) {
		this(config, clock, client(config), true, threadFactory);
	}

	protected AbstractRedisMeterRegistry(RedisConfig config, Clock clock, AbstractRedisClient client,
			ThreadFactory threadFactory) {
		this(config, clock, client, false, threadFactory);
	}

	private AbstractRedisMeterRegistry(RedisConfig config, Clock clock, AbstractRedisClient client,
			boolean shutdownClient, ThreadFactory threadFactory) {
		super(config, clock);
		this.config = config;
		this.client = client;
		this.shutdownClient = shutdownClient;
		this.connection = RedisModulesUtils.connection(client);
		this.connection.setAutoFlushCommands(false);
		config().namingConvention(new RedisNamingConvention(config.keySeparator()));
		start(threadFactory);
	}

	private static AbstractRedisClient client(RedisConfig config) {
		if (config.cluster()) {
			return RedisModulesClusterClient.create(config.uri());
		}
		return RedisModulesClient.create(config.uri());
	}

	protected RedisModulesAsyncCommands<String, String> async() {
		return connection.async();
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

	protected boolean addFuture(RedisFuture<?> future) {
		return futures.add(future);
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

	public boolean write(List<Meter> batch) throws InterruptedException {
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

	private boolean awaitAll() throws InterruptedException {
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
					handleExecutionException(e);
				}
			}
			return true;
		} catch (TimeoutException e) {
			return false;
		} catch (InterruptedException e) {
			throw e;
		} catch (Exception e) {
			throw Exceptions.fromSynchronization(e);
		}
	}

	protected void handleExecutionException(ExecutionException e) throws ExecutionException {
		throw e;
	}

	protected abstract void writeFunctionCounter(FunctionCounter counter);

	protected abstract void writeCounter(Counter counter);

	protected abstract void writeGauge(Gauge gauge);

	protected abstract void writeTimeGauge(TimeGauge timeGauge);

	protected abstract void writeLongTaskTimer(LongTaskTimer timer);

	protected abstract void writeCustomMetric(Meter meter);

	protected abstract void writeDistributionSummary(DistributionSummary summary);

	protected abstract void writeFunctionTimer(FunctionTimer timer);

	protected abstract void writeTimer(Timer timer);

	@Override
	protected TimeUnit getBaseTimeUnit() {
		return TimeUnit.MILLISECONDS;
	}

	protected String key(Id id) {
		return prefix(getConventionName(id));
	}

	protected String key(Id id, String suffix) {
		// usually tagKeys and metricNames naming rules are the same
		// but we can't call getConventionName again after adding suffix
		return prefix(config().namingConvention().tagKey(getConventionName(id) + "." + suffix));
	}

	private String prefix(String key) {
		return config.keyPrefix() + key;
	}

	@Override
	protected String getConventionName(Id id) {
		StringBuilder hierarchicalName = new StringBuilder();
		hierarchicalName.append(super.getConventionName(id));
		for (Tag tag : getConventionTags(id)) {
			hierarchicalName.append(config.keySeparator()).append(tag.getKey()).append(config.keySeparator())
					.append(tag.getValue());
		}
		return hierarchicalName.toString();

	}

}
