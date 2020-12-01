package com.redislabs.redistimeseries.micrometer;

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
import com.redislabs.redistimeseries.Range;
import com.redislabs.redistimeseries.RedisTimeSeries;
import com.redislabs.redistimeseries.Value;

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

	private RedisTimeSeriesMeterRegistry registry;
	private RedisTimeSeries rts;

	@Container
	@SuppressWarnings("rawtypes")
	public static final GenericContainer REDISTIMESERIES = new GenericContainer(
			DockerImageName.parse("redislabs/redistimeseries")).withExposedPorts(6379);

	@BeforeEach
	public void setup() {
		String host = REDISTIMESERIES.getHost();
		int port = REDISTIMESERIES.getFirstMappedPort();
		RedisURI redisURI = RedisURI.create(host, port);
		rts = new RedisTimeSeries(host, port);
		RedisTimeSeriesClient client = RedisTimeSeriesClient.create(redisURI);
		client.connect().sync().flushall();
		client.shutdown();
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
	}

	@AfterEach
	public void teardown() {
		registry.close();
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
	void writeGauge() throws Exception {
		registry.gauge("my.gauge", 1d);
		Gauge gauge = registry.get("my.gauge").gauge();
		registry.write(gauge);
		Value[] range = rts.range("my:gauge", System.currentTimeMillis() - 1000, System.currentTimeMillis());
		Assertions.assertTrue(range.length == 1);
		Assertions.assertEquals(range[0].getValue(), 1d);
	}

	@Test
	void writeGaugeShouldDropNanValue() throws Exception {
		registry.gauge("my.gauge", Double.NaN);
		Gauge gauge = registry.get("my.gauge").gauge();
		registry.write(gauge);
		assertKeyAbsent("my:gauge");
	}

	@Test
	void writeGaugeShouldDropInfiniteValues() throws Exception {
		registry.gauge("my.gauge", Double.POSITIVE_INFINITY);
		Gauge gauge = registry.get("my.gauge").gauge();
		registry.write(gauge);
		assertKeyAbsent("my:gauge");
		registry.gauge("my.gauge", Double.NEGATIVE_INFINITY);
		gauge = registry.get("my.gauge").gauge();
		registry.write(gauge);
		assertKeyAbsent("my:gauge");
	}

	@Test
	void writeTimeGauge() throws Exception {
		AtomicReference<Double> obj = new AtomicReference<>(1d);
		registry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		TimeGauge timeGauge = registry.get("my.time.gauge").timeGauge();
		registry.write(timeGauge);
		Value[] range = rts.range("my:time:gauge", System.currentTimeMillis() - 1000, System.currentTimeMillis());
		Assertions.assertTrue(range.length == 1);
		Assertions.assertEquals(1, range[0].getValue());

	}

	@Test
	void writeTimeGaugeShouldDropNanValue() throws Exception {
		AtomicReference<Double> obj = new AtomicReference<>(Double.NaN);
		registry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		TimeGauge timeGauge = registry.get("my.time.gauge").timeGauge();
		registry.write(timeGauge);
		assertKeyAbsent("my:time:gauge");
	}

	@Test
	void writeTimeGaugeShouldDropInfiniteValues() throws Exception {
		AtomicReference<Double> obj = new AtomicReference<>(Double.POSITIVE_INFINITY);
		registry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		TimeGauge timeGauge = registry.get("my.time.gauge").timeGauge();
		registry.write(timeGauge);
		assertKeyAbsent("my:time:gauge");
		obj = new AtomicReference<>(Double.NEGATIVE_INFINITY);
		registry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		timeGauge = registry.get("my.time.gauge").timeGauge();
		registry.write(timeGauge);
		assertKeyAbsent("my:time:gauge");
	}

	@Test
	void longTaskTimer() throws Exception {
		LongTaskTimer timer = LongTaskTimer.builder("my.timer").tag("tag", "value").register(registry);
		registry.write(timer);
		Range[] ranges = rts.mget(true, "tag=value");
		Assertions.assertEquals(3, ranges.length);
		Assertions.assertEquals("my:timer:active:count", ranges[0].getKey());
		Assertions.assertEquals("my:timer:duration:sum", ranges[1].getKey());
		Assertions.assertEquals("my:timer:max", ranges[2].getKey());
	}

}
