package com.redislabs.redistimeseries.micrometer;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.redislabs.mesclun.RedisTimeSeriesClient;
import com.redislabs.mesclun.api.StatefulRedisTimeSeriesConnection;
import com.redislabs.mesclun.api.async.RedisTimeSeriesAsyncCommands;
import com.redislabs.mesclun.api.reactive.RedisTimeSeriesReactiveCommands;
import com.redislabs.mesclun.api.sync.RedisTimeSeriesCommands;
import com.redislabs.redistimeseries.Range;
import com.redislabs.redistimeseries.RedisTimeSeries;
import com.redislabs.redistimeseries.Value;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.TimeGauge;
import redis.clients.jedis.exceptions.JedisDataException;

@Testcontainers
public class RedisTimeSeriesMeterRegistryTest {

	private final RedisTimeSeriesConfig config = RedisTimeSeriesConfig.DEFAULT;
	private final RedisTimeSeriesMeterRegistry meterRegistry = new RedisTimeSeriesMeterRegistry(config, Clock.SYSTEM);

	private RedisTimeSeriesClient client;
	protected StatefulRedisTimeSeriesConnection<String, String> connection;
	protected RedisTimeSeriesCommands<String, String> sync;
	protected RedisTimeSeriesAsyncCommands<String, String> async;
	protected RedisTimeSeriesReactiveCommands<String, String> reactive;

	protected String host;
	protected int port;
	protected RedisURI redisURI;
	private RedisTimeSeriesMeterRegistry registry;
	private RedisTimeSeries rts;

	@Container
	@SuppressWarnings("rawtypes")
	public static final GenericContainer REDISTIMESERIES = new GenericContainer(
			DockerImageName.parse("redislabs/redistimeseries")).withExposedPorts(6379);

	@BeforeEach
	public void setup() {
		host = REDISTIMESERIES.getHost();
		port = REDISTIMESERIES.getFirstMappedPort();
		redisURI = RedisURI.create(host, port);
		client = RedisTimeSeriesClient.create(redisURI);
		rts = new RedisTimeSeries(host, port);

		connection = client.connect();
		sync = connection.sync();
		async = connection.async();
		reactive = connection.reactive();
		sync.flushall();
		registry = new RedisTimeSeriesMeterRegistry(new RedisTimeSeriesConfig() {

			@Override
			public String get(String key) {
				return null;
			}

			@Override
			public RedisURI uri() {
				return redisURI;
			}

		}, Clock.SYSTEM);
		Metrics.addRegistry(registry);
		async.setAutoFlushCommands(false);
	}

	@AfterEach
	public void teardown() {
		if (connection != null) {
			connection.close();
		}
		if (client != null) {
			client.shutdown();
		}
	}

	private void flushAndAwaitAll(List<RedisFuture<Long>> futures) {
		async.flushCommands();
		LettuceFutures.awaitAll(redisURI.getTimeout(), futures.toArray(new RedisFuture[0]));
	}

	private void assertKeyAbsent(String key) {
		try {
			rts.get(key);
			Assertions.fail("Key should not exist");
		} catch (JedisDataException e) {
			// expected
		}
	}

	@Test
	void writeGauge() {
		meterRegistry.gauge("my.gauge", 1d);
		Gauge gauge = meterRegistry.get("my.gauge").gauge();
		flushAndAwaitAll(meterRegistry.writeGauge(async, gauge));
		Value[] range = rts.range("my:gauge", System.currentTimeMillis() - 1000, System.currentTimeMillis());
		Assertions.assertTrue(range.length == 1);
		Assertions.assertEquals(range[0].getValue(), 1d);
	}

	@Test
	void writeGaugeShouldDropNanValue() {
		meterRegistry.gauge("my.gauge", Double.NaN);
		Gauge gauge = meterRegistry.get("my.gauge").gauge();
		flushAndAwaitAll(meterRegistry.writeGauge(async, gauge));
		assertKeyAbsent("my:gauge");
	}

	@Test
	void writeGaugeShouldDropInfiniteValues() {
		meterRegistry.gauge("my.gauge", Double.POSITIVE_INFINITY);
		Gauge gauge = meterRegistry.get("my.gauge").gauge();
		flushAndAwaitAll(meterRegistry.writeGauge(async, gauge));
		assertKeyAbsent("my:gauge");
		meterRegistry.gauge("my.gauge", Double.NEGATIVE_INFINITY);
		gauge = meterRegistry.get("my.gauge").gauge();
		flushAndAwaitAll(meterRegistry.writeGauge(async, gauge));
		assertKeyAbsent("my:gauge");
	}

	@Test
	void writeTimeGauge() {
		AtomicReference<Double> obj = new AtomicReference<>(1d);
		meterRegistry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		TimeGauge timeGauge = meterRegistry.get("my.time.gauge").timeGauge();
		flushAndAwaitAll(meterRegistry.writeTimeGauge(async, timeGauge));
		Value[] range = rts.range("my:time:gauge", System.currentTimeMillis() - 1000, System.currentTimeMillis());
		Assertions.assertTrue(range.length == 1);
		Assertions.assertEquals(1, range[0].getValue());

	}

	@Test
	void writeTimeGaugeShouldDropNanValue() {
		AtomicReference<Double> obj = new AtomicReference<>(Double.NaN);
		meterRegistry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		TimeGauge timeGauge = meterRegistry.get("my.time.gauge").timeGauge();
		flushAndAwaitAll(meterRegistry.writeTimeGauge(async, timeGauge));
		assertKeyAbsent("my:time:gauge");
	}

	@Test
	void writeTimeGaugeShouldDropInfiniteValues() {
		AtomicReference<Double> obj = new AtomicReference<>(Double.POSITIVE_INFINITY);
		meterRegistry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		TimeGauge timeGauge = meterRegistry.get("my.time.gauge").timeGauge();
		flushAndAwaitAll(meterRegistry.writeTimeGauge(async, timeGauge));
		assertKeyAbsent("my:time:gauge");

		obj = new AtomicReference<>(Double.NEGATIVE_INFINITY);
		meterRegistry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		timeGauge = meterRegistry.get("my.time.gauge").timeGauge();
		flushAndAwaitAll(meterRegistry.writeTimeGauge(async, timeGauge));
		assertKeyAbsent("my:time:gauge");
	}

	@Test
	void longTaskTimer() {
		LongTaskTimer timer = LongTaskTimer.builder("my.timer").tag("tag", "value").register(meterRegistry);
		flushAndAwaitAll(meterRegistry.writeLongTaskTimer(async, timer));
		Range[] ranges = rts.mget(true, "tag=value");
		Assertions.assertEquals(3, ranges.length);
		Assertions.assertEquals("my:timer:active:count", ranges[0].getKey());
		Assertions.assertEquals("my:timer:duration:sum", ranges[1].getKey());
		Assertions.assertEquals("my:timer:max", ranges[2].getKey());
	}

}
