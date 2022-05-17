package com.redis.micrometer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.timeseries.Aggregation;
import com.redis.lettucemod.timeseries.Aggregation.Aggregator;
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.MRangeOptions;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.testcontainers.RedisModulesContainer;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

@Testcontainers
class RedisTimeSeriesMeterRegistryTests {

	@Container
	private static final RedisModulesContainer REDIS = new RedisModulesContainer(
			RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG));

	private static RedisModulesClient client;

	private static StatefulRedisModulesConnection<String, String> connection;

	private final RedisTimeSeriesMeterRegistry registry = new RedisTimeSeriesMeterRegistry(new RedisTimeSeriesConfig() {

		@Override
		public String get(String key) {
			return null;
		}

		@Override
		public String uri() {
			return REDIS.getRedisURI();
		}

	}, Clock.SYSTEM);

	@BeforeAll
	public static void setup() {
		client = RedisModulesClient.create(REDIS.getRedisURI());
		connection = client.connect();
	}

	@AfterAll
	public static void teardown() {
		connection.close();
		client.shutdown();
		client.getResources().shutdown();
	}

	@BeforeEach
	public void flushAll() {
		connection.sync().flushall();
	}

	private void assertKeyAbsent(String key) {
		Assertions.assertEquals(0, sync().exists(key));
	}

	private RedisModulesCommands<String, String> sync() {
		return connection.sync();
	}

	@Test
	void writeGauge() throws Exception {
		registry.gauge("my.gauge", 1d);
		registry.write(registry.get("my.gauge").gauge());
		List<Sample> samples = sync().range("my:gauge",
				RangeOptions.range(System.currentTimeMillis() - 1000, System.currentTimeMillis()).build());
		Assertions.assertTrue(samples.size() == 1);
		Assertions.assertEquals(samples.get(0).getValue(), 1d);
	}

	@Test
	void writeGaugeShouldDropNanValue() throws Exception {
		registry.gauge("my.gauge", Double.NaN);
		registry.write(registry.get("my.gauge").gauge());
		assertKeyAbsent("my:gauge");
	}

	@Test
	void writeGaugeShouldDropInfiniteValues() throws Exception {
		registry.gauge("my.gauge", Double.POSITIVE_INFINITY);
		registry.write(registry.get("my.gauge").gauge());
		assertKeyAbsent("my:gauge");
		registry.gauge("my.gauge", Double.NEGATIVE_INFINITY);
		registry.write(registry.get("my.gauge").gauge());
		assertKeyAbsent("my:gauge");
	}

	@Test
	void writeTimeGauge() throws Exception {
		AtomicReference<Double> obj = new AtomicReference<>(1d);
		registry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		registry.write(registry.get("my.time.gauge").timeGauge());
		List<Sample> samples = connection.sync().range("my:time:gauge",
				RangeOptions.range(System.currentTimeMillis() - 1000, System.currentTimeMillis()).build());
		Assertions.assertTrue(samples.size() == 1);
		Assertions.assertEquals(1000, samples.get(0).getValue());
	}

	@Test
	void writeTimeGaugeShouldDropNanValue() throws Exception {
		AtomicReference<Double> obj = new AtomicReference<>(Double.NaN);
		registry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		registry.write(registry.get("my.time.gauge").timeGauge());
		assertKeyAbsent("my:time:gauge");
	}

	@Test
	void writeTimeGaugeShouldDropInfiniteValues() throws Exception {
		AtomicReference<Double> obj = new AtomicReference<>(Double.POSITIVE_INFINITY);
		registry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		registry.write(registry.get("my.time.gauge").timeGauge());
		assertKeyAbsent("my:time:gauge");
		obj = new AtomicReference<>(Double.NEGATIVE_INFINITY);
		registry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		registry.write(registry.get("my.time.gauge").timeGauge());
		assertKeyAbsent("my:time:gauge");
	}

	@Test
	void writeLongTaskTimer() throws Exception {
		LongTaskTimer timer = LongTaskTimer.builder("my.timer").tag("tag", "value").register(registry);
		registry.write(timer);
		List<GetResult<String, String>> results = connection.sync().tsMget("tag=value");
		Assertions.assertEquals(3, results.size());
		Assertions.assertEquals("my:timer:tag:value:active:count", results.get(0).getKey());
		Assertions.assertEquals("my:timer:tag:value:duration:sum", results.get(1).getKey());
		Assertions.assertEquals("my:timer:tag:value:max", results.get(2).getKey());
	}

	@Test
	void testLabels() throws Exception {
		registry.gauge("my.gauge", Arrays.asList(Tag.of("tag1", "value"), Tag.of("tag2", "value")), 1d);
		registry.write(registry.get("my.gauge").gauge());
		List<GetResult<String, String>> getResults = sync().tsMgetWithLabels("tag2=value");
		Assertions.assertEquals(1, getResults.size());
		Assertions.assertEquals(1d, getResults.get(0).getSample().getValue());
		List<RangeResult<String, String>> results = sync().mrange(
				MRangeOptions.<String, String>range(System.currentTimeMillis() - 1000, System.currentTimeMillis())
						.filters("tag1=value").aggregation(Aggregation.builder(Aggregator.AVG, 1).build()).build());
		Assertions.assertEquals(1, results.size());
		Assertions.assertEquals(1, results.get(0).getSamples().size());
		Assertions.assertEquals(1d, results.get(0).getSamples().get(0).getValue());

	}
}
