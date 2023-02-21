package com.redis.micrometer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.timeseries.Aggregation;
import com.redis.lettucemod.timeseries.Aggregator;
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.MRangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisURI;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

@Testcontainers
@TestInstance(Lifecycle.PER_CLASS)
abstract class BaseRegistryTests {

	private AbstractRedisClient client;
	private StatefulRedisModulesConnection<String, String> connection;
	private RedisTimeSeriesMeterRegistry registry;

	@BeforeAll
	public void setup() {
		client = ClientBuilder.create(RedisURI.create(getRedisServer().getRedisURI()))
				.cluster(getRedisServer().isCluster()).build();
		connection = RedisModulesUtils.connection(client);
	}

	abstract RedisServer getRedisServer();

	@AfterAll
	public void teardown() {
		if (connection != null) {
			connection.close();
		}
		if (client != null) {
			client.shutdown();
			client.getResources().shutdown();
		}
	}

	@BeforeEach
	public void setupRegistry() {
		connection.sync().flushall();
		registry = new RedisTimeSeriesMeterRegistry(new RedisTimeSeriesConfig() {

			@Override
			public String get(String key) {
				return null;
			}

			@Override
			public boolean enabled() {
				return false;
			}

		}, Clock.SYSTEM, client);
	}

	@AfterEach
	public void teardownRegistry() {
		registry.close();
	}

	private void assertNoSamples(String id) {
		Assertions.assertNull(connection.sync().tsGet(key(id)));
	}

	private String key(String id) {
		return id.replace('.', ':');
	}

	@Test
	void writeGauge() throws Exception {
		String id = "writegauge.gauge";
		registry.gauge(id, 1d);
		registry.write(registry.get(id).gauge());
		List<Sample> samples = new ArrayList<>();
		try {
			samples = connection.sync().tsRange(key(id),
					TimeRange.from(System.currentTimeMillis() - 1000).to(System.currentTimeMillis()).build());
		} catch (RedisCommandExecutionException e) {
			System.out.println(connection.sync().keys("*"));
		}
		Assertions.assertEquals(1, samples.size());
		Assertions.assertEquals(1d, samples.get(0).getValue());
	}

	@Test
	void writeCounter() throws Exception {
		String id = "writecounter.counter";
		registry.counter(id).increment();
		registry.write(registry.get(id).counter());
		List<Sample> samples = connection.sync().tsRange(key(id), TimeRange.unbounded());
		Assertions.assertEquals(1, samples.size());
	}

	@Test
	void writeGaugeShouldDropNanValue() throws Exception {
		String id = "writegaugedropnan.gauge";
		registry.gauge(id, Double.NaN);
		registry.write(registry.get(id).gauge());

		assertNoSamples(id);
	}

	@Test
	void writeGaugeShouldDropInfiniteValues() throws Exception {
		String id = "writegaugedropinfinite.gauge";
		registry.gauge(id, Double.POSITIVE_INFINITY);
		registry.write(registry.get(id).gauge());
		assertNoSamples(key(id));
		registry.gauge(id, Double.NEGATIVE_INFINITY);
		registry.write(registry.get(id).gauge());
		assertNoSamples(key(id));
	}

	@Test
	void writeTimeGauge() throws Exception {
		AtomicReference<Double> obj = new AtomicReference<>(1d);
		String id = "writetimegauge.time.gauge";
		registry.more().timeGauge(id, Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		registry.write(registry.get(id).timeGauge());
		List<Sample> samples = connection.sync().tsRange(key(id),
				TimeRange.from(System.currentTimeMillis() - 1000).to(System.currentTimeMillis()).build());
		Assertions.assertEquals(1, samples.size());
		Assertions.assertEquals(1000, samples.get(0).getValue());
	}

	@Test
	void writeTimeGaugeShouldDropNanValue() throws Exception {
		AtomicReference<Double> obj = new AtomicReference<>(Double.NaN);
		String id = "writetimegaugedropnan.time.gauge";
		registry.more().timeGauge(id, Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		registry.write(registry.get(id).timeGauge());
		assertNoSamples(id);
	}

	@Test
	void writeTimeGaugeShouldDropInfiniteValues() throws Exception {
		String id = "writetimegaugedropinfinite.time.gauge";
		AtomicReference<Double> obj = new AtomicReference<>(Double.POSITIVE_INFINITY);
		registry.more().timeGauge(id, Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		registry.write(registry.get(id).timeGauge());
		assertNoSamples(id);
		obj = new AtomicReference<>(Double.NEGATIVE_INFINITY);
		registry.more().timeGauge(id, Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
		registry.write(registry.get(id).timeGauge());
		assertNoSamples(id);
	}

	@Test
	void writeLongTaskTimer() throws Exception {
		String id = "writelongtasktimer.timer";
		LongTaskTimer timer = LongTaskTimer.builder(id).tag("tag", "value").register(registry);
		registry.write(timer);
		List<GetResult<String, String>> results = connection.sync().tsMget("tag=value");
		Assertions.assertEquals(3, results.size());
		String prefix = id + ".tag.value";
		Assertions.assertEquals(key(prefix + ".active.count"), results.get(0).getKey());
		Assertions.assertEquals(key(prefix + ".duration.sum"), results.get(1).getKey());
		Assertions.assertEquals(key(prefix + ".max"), results.get(2).getKey());
	}

	@Test
	void testLabels() throws Exception {
		String id = "testlabels.meter";
		registry.gauge(id, Arrays.asList(Tag.of("tag1", "value"), Tag.of("tag2", "value")), 1d);
		registry.write(registry.get(id).gauge());
		List<GetResult<String, String>> getResults = connection.sync().tsMgetWithLabels("tag2=value");
		Assertions.assertEquals(1, getResults.size());
		Assertions.assertEquals(1d, getResults.get(0).getSample().getValue());
		List<RangeResult<String, String>> results = connection.sync().tsMrange(
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
