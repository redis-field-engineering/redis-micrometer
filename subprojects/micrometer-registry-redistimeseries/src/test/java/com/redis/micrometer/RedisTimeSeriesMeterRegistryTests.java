package com.redis.micrometer;

import java.time.Duration;
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
import com.redis.lettucemod.timeseries.Aggregator;
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.MRangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.testcontainers.RedisStackContainer;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

@Testcontainers
class RedisTimeSeriesMeterRegistryTests {

	@Container
	private static final RedisStackContainer REDIS = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	private static RedisModulesClient client;

	private static StatefulRedisModulesConnection<String, String> connection;

	private final RedisTimeSeriesConfig config = new RedisTimeSeriesConfig() {

		@Override
		public String get(String key) {
			return null;
		}

		@Override
		public String uri() {
			return REDIS.getRedisURI();
		}

	};

	private final RedisTimeSeriesMeterRegistry registry = new RedisTimeSeriesMeterRegistry(config, Clock.SYSTEM);

	private static RedisModulesCommands<String, String> sync;

	@BeforeAll
	public static void setup() {
		client = RedisModulesClient.create(REDIS.getRedisURI());
		connection = client.connect();
		sync = connection.sync();
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

	private void assertNoSamples(String key) {
		Assertions.assertNull(sync.tsGet(key));
	}

	@Test
	void writeGauge() throws Exception {
		registry.gauge("my.gauge", 1d);
		registry.write(registry.get("my.gauge").gauge());
		List<Sample> samples = sync.tsRange("my:gauge",
				TimeRange.from(System.currentTimeMillis() - 1000).to(System.currentTimeMillis()).build());
		Assertions.assertEquals(1, samples.size());
		Assertions.assertEquals(1d, samples.get(0).getValue());
	}

	@Test
	void writeCounter() throws Exception {
		registry.counter("my.counter").increment();
		registry.write(registry.get("my.counter").counter());
		List<Sample> samples = sync.tsRange("my:counter", TimeRange.unbounded());
		Assertions.assertEquals(1, samples.size());
	}

	@Test
	void writeGaugeShouldDropNanValue() throws Exception {
		registry.gauge("my.gauge", Double.NaN);
		registry.write(registry.get("my.gauge").gauge());

		assertNoSamples("my:gauge");
	}

	@Test
	void writeGaugeShouldDropInfiniteValues() throws Exception {
		registry.gauge("my.gauge", Double.POSITIVE_INFINITY);
		registry.write(registry.get("my.gauge").gauge());
		assertNoSamples("my:gauge");
		registry.gauge("my.gauge", Double.NEGATIVE_INFINITY);
		registry.write(registry.get("my.gauge").gauge());
		assertNoSamples("my:gauge");
	}

	@Test
	void writeTimeGauge() throws Exception {
		AtomicReference<Double> obj = new AtomicReference<>(1d);
		registry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		registry.write(registry.get("my.time.gauge").timeGauge());
		List<Sample> samples = sync.tsRange("my:time:gauge",
				TimeRange.from(System.currentTimeMillis() - 1000).to(System.currentTimeMillis()).build());
		Assertions.assertEquals(1, samples.size());
		Assertions.assertEquals(1000, samples.get(0).getValue());
	}

	@Test
	void writeTimeGaugeShouldDropNanValue() throws Exception {
		AtomicReference<Double> obj = new AtomicReference<>(Double.NaN);
		registry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		registry.write(registry.get("my.time.gauge").timeGauge());
		assertNoSamples("my:time:gauge");
	}

	@Test
	void writeTimeGaugeShouldDropInfiniteValues() throws Exception {
		AtomicReference<Double> obj = new AtomicReference<>(Double.POSITIVE_INFINITY);
		registry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		registry.write(registry.get("my.time.gauge").timeGauge());
		assertNoSamples("my:time:gauge");
		obj = new AtomicReference<>(Double.NEGATIVE_INFINITY);
		registry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		registry.write(registry.get("my.time.gauge").timeGauge());
		assertNoSamples("my:time:gauge");
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
		List<GetResult<String, String>> getResults = sync.tsMgetWithLabels("tag2=value");
		Assertions.assertEquals(1, getResults.size());
		Assertions.assertEquals(1d, getResults.get(0).getSample().getValue());
		List<RangeResult<String, String>> results = sync.tsMrange(
				TimeRange.from(System.currentTimeMillis() - 1000).to(System.currentTimeMillis()).build(),
				MRangeOptions.<String, String>filters("tag1=value")
						.aggregation(
								Aggregation.aggregator(Aggregator.AVG).bucketDuration(Duration.ofMillis(1)).build())
						.build());
		Assertions.assertEquals(1, results.size());
		Assertions.assertEquals(1, results.get(0).getSamples().size());
		Assertions.assertEquals(1d, results.get(0).getSamples().get(0).getValue());
	}

	@Test
	void testCustomMetric() throws Exception {
		Measurement m1 = new Measurement(() -> 23d, Statistic.VALUE);
		Measurement m2 = new Measurement(() -> 13d, Statistic.VALUE);
		Measurement m3 = new Measurement(() -> 5d, Statistic.TOTAL_TIME);
		Meter meter = Meter.builder("my.custom", Meter.Type.OTHER, Arrays.asList(m1, m2, m3)).tag("mytag", "value")
				.register(registry);
		registry.write(meter);
		Assertions.assertEquals(2, connection.sync()
				.tsMrange(TimeRange.unbounded(), MRangeOptions.<String, String>filters("mytag=value").build()).size());
	}

}
