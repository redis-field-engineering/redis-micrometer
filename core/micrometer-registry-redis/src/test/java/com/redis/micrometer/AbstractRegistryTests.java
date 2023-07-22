package com.redis.micrometer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.SearchOptions;
import com.redis.lettucemod.search.SearchOptions.SortBy;
import com.redis.lettucemod.search.SearchResults;
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
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;

@Testcontainers
@TestInstance(Lifecycle.PER_CLASS)
abstract class AbstractRegistryTests {

	private AbstractRedisClient client;
	protected StatefulRedisModulesConnection<String, String> connection;
	protected RedisTimeSeriesMeterRegistry tsRegistry;
	protected RediSearchMeterRegistry searchRegistry;

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
		tsRegistry = new RedisTimeSeriesMeterRegistry(new RedisRegistryConfig() {

			@Override
			public String get(String key) {
				return null;
			}

			@Override
			public boolean enabled() {
				return false;
			}

		}, Clock.SYSTEM, client);
		searchRegistry = new RediSearchMeterRegistry(new RediSearchRegistryConfig() {

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
		tsRegistry.close();
		searchRegistry.close();
	}

	private void assertNoSamples(String id) {
		Assertions.assertNull(connection.sync().tsGet(key(id)));
	}

	private String key(String id) {
		return id.replace('.', ':');
	}

	@Nested
	class TimeSeriesTests {

		@Test
		void writeGauge() throws Exception {
			String id = "writegauge.gauge";
			tsRegistry.gauge(id, 1d);
			tsRegistry.write(tsRegistry.get(id).gauge());
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
			tsRegistry.counter(id).increment();
			tsRegistry.write(tsRegistry.get(id).counter());
			List<Sample> samples = connection.sync().tsRange(key(id), TimeRange.unbounded());
			Assertions.assertEquals(1, samples.size());
		}

		@Test
		void writeTimer() throws Exception {
			String id = "writetimer.timer";
			tsRegistry.timer(id).record(Duration.ofMillis(3));
			tsRegistry.write(tsRegistry.get(id).timer());
			List<Sample> samples = connection.sync().tsRange(key(id) + ":count", TimeRange.unbounded());
			Assertions.assertEquals(1, samples.size());
		}

		@Test
		void writeTimerWithTags() throws Exception {
			String id = "writeTimerWithTags.timer";
			String tagName = "mytag";
			String tagValue = "tagvalue";
			tsRegistry.timer(id, tagName, tagValue).record(Duration.ofMillis(3));
			tsRegistry.write(tsRegistry.get(id).tags(tagName, tagValue).timer());
			List<Sample> samples = connection.sync().tsRange(key(id) + ":" + tagValue + ":count",
					TimeRange.unbounded());
			Assertions.assertEquals(1, samples.size());
		}

		@Test
		void writeGaugeShouldDropNanValue() throws Exception {
			String id = "writegaugedropnan.gauge";
			tsRegistry.gauge(id, Double.NaN);
			tsRegistry.write(tsRegistry.get(id).gauge());

			assertNoSamples(id);
		}

		@Test
		void writeGaugeShouldDropInfiniteValues() throws Exception {
			String id = "writegaugedropinfinite.gauge";
			tsRegistry.gauge(id, Double.POSITIVE_INFINITY);
			tsRegistry.write(tsRegistry.get(id).gauge());
			assertNoSamples(key(id));
			tsRegistry.gauge(id, Double.NEGATIVE_INFINITY);
			tsRegistry.write(tsRegistry.get(id).gauge());
			assertNoSamples(key(id));
		}

		@Test
		void writeTimeGauge() throws Exception {
			AtomicReference<Double> obj = new AtomicReference<>(1d);
			String id = "writetimegauge.time.gauge";
			tsRegistry.more().timeGauge(id, Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
			tsRegistry.write(tsRegistry.get(id).timeGauge());
			List<Sample> samples = connection.sync().tsRange(key(id),
					TimeRange.from(System.currentTimeMillis() - 1000).to(System.currentTimeMillis()).build());
			Assertions.assertEquals(1, samples.size());
			Assertions.assertEquals(1000, samples.get(0).getValue());
		}

		@Test
		void writeTimeGaugeShouldDropNanValue() throws Exception {
			AtomicReference<Double> obj = new AtomicReference<>(Double.NaN);
			String id = "writetimegaugedropnan.time.gauge";
			tsRegistry.more().timeGauge(id, Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
			tsRegistry.write(tsRegistry.get(id).timeGauge());
			assertNoSamples(id);
		}

		@Test
		void writeTimeGaugeShouldDropInfiniteValues() throws Exception {
			String id = "writetimegaugedropinfinite.time.gauge";
			AtomicReference<Double> obj = new AtomicReference<>(Double.POSITIVE_INFINITY);
			tsRegistry.more().timeGauge(id, Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
			tsRegistry.write(tsRegistry.get(id).timeGauge());
			assertNoSamples(id);
			obj = new AtomicReference<>(Double.NEGATIVE_INFINITY);
			tsRegistry.more().timeGauge(id, Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
			tsRegistry.write(tsRegistry.get(id).timeGauge());
			assertNoSamples(id);
		}

		@Test
		void writeLongTaskTimer() throws Exception {
			String id = "writelongtasktimer.timer";
			LongTaskTimer timer = LongTaskTimer.builder(id).tag("tag", "value").register(tsRegistry);
			tsRegistry.write(timer);
			String prefix = id + ".value";
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
					.register(tsRegistry);
			tsRegistry.write(meter);
			Assertions.assertEquals(2, connection.sync()
					.tsMrange(TimeRange.unbounded(), MRangeOptions.<String, String>filters("mytag=value").build())
					.size());
		}

		@Test
		void testLabels() throws Exception {
			String id = "testlabels.meter";
			Tag tag1 = Tag.of("tag1", "value");
			Tag tag2 = Tag.of("tag2", "value");
			tsRegistry.timer(id, Arrays.asList(tag1, tag2)).record(Duration.ofMillis(3));
			tsRegistry.write(tsRegistry.get(id).timer());
			List<GetResult<String, String>> results = connection.sync().tsMgetWithLabels("tag2=value");
			Assertions.assertEquals(4, results.size());
			Assertions.assertEquals(4, results.get(0).getLabels().size());
			Assertions.assertTrue(results.get(0).getLabels().contains(Label.of(tag1.getKey(), tag2.getValue())));
		}
	}

	@Nested
	class SearchTests {

		@Test
		void writeTimer() throws Exception {
			String id = "writetimer.timer";
			long value1 = 10;
			searchRegistry.timer(id).record(Duration.ofMillis(value1));
			searchRegistry.write(searchRegistry.get(id).timer());
			SearchResults<String, String> results = connection.sync().ftSearch("writetimer-timer", "*");
			Assertions.assertEquals(1, results.size());
			Document<String, String> result1 = results.get(0);
			Assertions.assertEquals(5, result1.size());
			Assertions.assertEquals(10, Double.parseDouble(results.get(0).get(RediSearchMeterRegistry.FIELD_MAX)));
		}

		@Test
		void testLabels() throws Exception {
			String id = "testlabels.meter";
			Tag tag1 = Tag.of("tag1", "value");
			Tag tag2 = Tag.of("tag2", "value");
			searchRegistry.timer(id, Arrays.asList(tag1, tag2)).record(Duration.ofMillis(3));
			searchRegistry.write(searchRegistry.get(id).timer());
			SearchResults<String, String> results = connection.sync().ftSearch("testlabels-meter", "@tag2:{value}");
			Assertions.assertEquals(1, results.size());
			Assertions.assertTrue(results.get(0).containsKey(tag1.getKey()));
			Assertions.assertEquals(tag1.getValue(), results.get(0).get(tag1.getKey()));
		}

		@Test
		void testSmartCache() throws InterruptedException {
			String id = "query";
			String tableTagKey = "table";
			Tag tableTag1 = Tag.of(tableTagKey, "table1");
			Tag tableTag2 = Tag.of(tableTagKey, "table1,table2");
			String queryTagKey = "query";
			Timer timer1 = searchRegistry.timer(id, Tags.of(Tag.of(queryTagKey, "1"), tableTag1));
			Timer timer2 = searchRegistry.timer(id, Tags.of(Tag.of(queryTagKey, "2"), tableTag2));
			timer1.record(Duration.ofSeconds(2));
			timer2.record(Duration.ofSeconds(3));
			searchRegistry.write(timer1, timer2);
			String index = "query";
			SearchResults<String, String> results = connection.sync().ftSearch(index, "*");
			Assertions.assertEquals(2, results.size());
			for (Document<String, String> doc : results) {
				Assertions.assertTrue(doc.containsKey(tableTagKey));
				Assertions.assertTrue(doc.containsKey(queryTagKey));
			}
		}

	}

	@Test
	void compositeRegistry() {
		Duration step = Duration.ofMillis(100);
		RedisTimeSeriesMeterRegistry tsRegistry = new RedisTimeSeriesMeterRegistry(new RedisRegistryConfig() {

			@Override
			public String keyspace() {
				return "ts";
			}

			@Override
			public String get(String key) {
				return null;
			}

			@Override
			public boolean enabled() {
				return true;
			}

			@Override
			public Duration step() {
				return step;
			}

		}, Clock.SYSTEM, client);
		tsRegistry.config().meterFilter(MeterFilter.ignoreTags("sql", "table"));
		String[] EXCLUDED_TAGS = { "sql", "table" };
		RediSearchMeterRegistry searchRegistry = new RediSearchMeterRegistry(new RediSearchRegistryConfig() {

			@Override
			public String keyspace() {
				return "hash";
			}

			@Override
			public String get(String key) {
				return null;
			}

			@Override
			public boolean enabled() {
				return true;
			}

			@Override
			public Duration step() {
				return step;
			}

			@Override
			public String[] nonKeyTags() {
				return EXCLUDED_TAGS;
			}
		}, Clock.SYSTEM, client);
		searchRegistry.config().meterFilter(MeterFilter.acceptNameStartsWith("query")).meterFilter(MeterFilter.deny());
		CompositeMeterRegistry registry = new CompositeMeterRegistry();
		registry.add(tsRegistry).add(searchRegistry);
		Timer timer1 = Timer.builder("query").tags("id", "123", "sql",
				"SELECT * FROM customers c, products p WHERE c.id = p.id", "table", "customers,products")
				.publishPercentiles(0.9, 0.99).register(registry);
		timer1.record(Duration.ofSeconds(3));
		Timer timer2 = Timer.builder("query").tag("id", "456").tag("sql", "SELECT * FROM customers c")
				.tag("table", "customers").publishPercentiles(0.9, 0.99).register(registry);
		timer2.record(Duration.ofSeconds(5));
		String index = "query";
		Awaitility.await().timeout(Duration.ofSeconds(3))
				.until(() -> connection.sync().ftSearch(index, "*").size() == 2);
		List<String> tsKeys = connection.sync().keys("ts:*");
		Assertions.assertEquals(
				new HashSet<>(
						Arrays.asList("ts:query:456:max", "ts:query:456:0.9", "ts:query:123:sum", "ts:query:123:0.99",
								"ts:query:123:count", "ts:query:123:0.9", "ts:query:456:count", "ts:query:123:mean",
								"ts:query:123:max", "ts:query:456:mean", "ts:query:456:sum", "ts:query:456:0.99")),
				new HashSet<>(tsKeys));
		List<String> searchKeys = connection.sync().keys("hash:*");
		Assertions.assertEquals(new HashSet<>(Arrays.asList("hash:query:456", "hash:query:123")),
				new HashSet<>(searchKeys));
		SearchOptions.Builder<String, String> options = SearchOptions.builder();
		options.sortBy(SortBy.asc("id"));
		SearchResults<String, String> results = connection.sync().ftSearch(index, "@table:{customers}",
				options.build());
		Assertions.assertEquals(2, results.size());
		Assertions.assertEquals("123", results.get(0).get("id"));
		Assertions.assertEquals("customers,products", results.get(0).get("table"));
		registry.close();
	}

}
