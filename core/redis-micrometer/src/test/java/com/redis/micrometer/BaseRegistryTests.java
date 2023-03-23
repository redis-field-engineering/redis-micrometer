package com.redis.micrometer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.Label;
import com.redis.lettucemod.timeseries.MRangeOptions;
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
	protected StatefulRedisModulesConnection<String, String> connection;
	protected RedisTimeSeriesMeterRegistry registry;

	@BeforeAll
	public void setup() {
		RedisServer server = getRedisServer();
		server.start();
		RedisURI uri = RedisURI.create(server.getRedisURI());
		client = ClientBuilder.create(uri).cluster(server.isCluster()).build();
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
		getRedisServer().stop();
	}

	@BeforeEach
	public void setupRegistry() {
		connection.sync().flushall();
		registry = new RedisTimeSeriesMeterRegistry(new RedisConfig() {

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
	void writeTimer() throws Exception {
		String id = "writetimer.timer";
		registry.timer(id).record(Duration.ofMillis(3));
		registry.write(registry.get(id).timer());
		List<Sample> samples = connection.sync().tsRange(key(id) + ":count", TimeRange.unbounded());
		Assertions.assertEquals(1, samples.size());
	}

	@Test
	void writeTimerWithTags() throws Exception {
		String id = "writeTimerWithTags.timer";
		String tagName = "mytag";
		String tagValue = "tagvalue";
		registry.timer(id, tagName, tagValue).record(Duration.ofMillis(3));
		registry.write(registry.get(id).tags(tagName, tagValue).timer());
		List<Sample> samples = connection.sync().tsRange(key(id) + ":" + tagName + ":" + tagValue + ":count",
				TimeRange.unbounded());
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
		String prefix = id + ".tag.value";
		Set<String> expected = Stream
				.of(key(prefix + ".active.count"), key(prefix + ".duration.sum"), key(prefix + ".max"))
				.collect(Collectors.toSet());
		List<GetResult<String, String>> results = connection.sync().tsMget("tag=value");
		Assertions.assertEquals(3, results.size());
		Assertions.assertEquals(expected, results.stream().map(GetResult::getKey).collect(Collectors.toSet()));

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

	@Test
	void testLabels() throws Exception {
		String id = "testlabels.meter";
		Tag tag1 = Tag.of("tag1", "value");
		Tag tag2 = Tag.of("tag2", "value");
		registry.timer(id, Arrays.asList(tag1, tag2)).record(Duration.ofMillis(3));
		registry.write(registry.get(id).timer());
		List<GetResult<String, String>> results = connection.sync().tsMgetWithLabels("tag2=value");
		Assertions.assertEquals(4, results.size());
		Assertions.assertEquals(4, results.get(0).getLabels().size());
		Assertions.assertTrue(results.get(0).getLabels().contains(Label.of(tag1.getKey(), tag2.getValue())));
	}

}
