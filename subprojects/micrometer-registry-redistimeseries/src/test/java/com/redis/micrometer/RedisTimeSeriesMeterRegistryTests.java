package com.redis.micrometer;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.springframework.util.unit.DataSize;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisModulesContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.TimeGauge;

class RedisTimeSeriesMeterRegistryTests extends AbstractTestcontainersRedisTestBase {

	@SuppressWarnings("resource")
	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(
				new RedisModulesContainer(
						RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG)),
				new RedisEnterpriseContainer(
						RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag(RedisEnterpriseContainer.DEFAULT_TAG))
						.withDatabase(Database.name("MicrometerTests").memory(DataSize.ofMegabytes(300))
								.ossCluster(true).modules(RedisModule.TIMESERIES).build()));
	}

	private RedisTimeSeriesMeterRegistry registry(RedisTestContext context) {
		RedisTimeSeriesMeterRegistry registry = new RedisTimeSeriesMeterRegistry(new RedisTimeSeriesConfig() {

			@Override
			public String get(String key) {
				return null;
			}

			@Override
			public String uri() {
				return context.getRedisURI();
			}

		}, Clock.SYSTEM);
		Metrics.addRegistry(registry);
		return registry;
	}

	private void assertKeyAbsent(RedisTestContext context, String key) {
		Assertions.assertNull(context.sync().get(key));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testGauge(RedisTestContext context) throws Exception {
		RedisTimeSeriesMeterRegistry registry = registry(context);
		try {
			registry.gauge("my.gauge", 1d);
			Gauge gauge = registry.get("my.gauge").gauge();
			registry.write(gauge);
			List<Sample> samples = context.sync().range("my:gauge",
					RangeOptions.range(System.currentTimeMillis() - 1000, System.currentTimeMillis()).build());
			Assertions.assertTrue(samples.size() == 1);
			Assertions.assertEquals(samples.get(0).getValue(), 1d);
		} finally {
			registry.close();
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testGaugeShouldDropNanValue(RedisTestContext context) throws Exception {
		RedisTimeSeriesMeterRegistry registry = registry(context);
		try {
			registry.gauge("my.gauge", Double.NaN);
			Gauge gauge = registry.get("my.gauge").gauge();
			registry.write(gauge);
			assertKeyAbsent(context, "my:gauge");
		} finally {
			registry.close();
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testGaugeShouldDropInfiniteValues(RedisTestContext context) throws Exception {
		RedisTimeSeriesMeterRegistry registry = registry(context);
		try {
			registry.gauge("my.gauge", Double.POSITIVE_INFINITY);
			Gauge gauge = registry.get("my.gauge").gauge();
			registry.write(gauge);
			assertKeyAbsent(context, "my:gauge");
			registry.gauge("my.gauge", Double.NEGATIVE_INFINITY);
			gauge = registry.get("my.gauge").gauge();
			registry.write(gauge);
			assertKeyAbsent(context, "my:gauge");
		} finally {
			registry.close();
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testTimeGauge(RedisTestContext context) throws Exception {
		RedisTimeSeriesMeterRegistry registry = registry(context);
		try {
			AtomicReference<Double> obj = new AtomicReference<>(1d);
			registry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
			TimeGauge timeGauge = registry.get("my.time.gauge").timeGauge();
			registry.write(timeGauge);
			List<Sample> samples = context.sync().range("my:time:gauge",
					RangeOptions.range(System.currentTimeMillis() - 1000, System.currentTimeMillis()).build());
			Assertions.assertTrue(samples.size() == 1);
			Assertions.assertEquals(1, samples.get(0).getValue());
		} finally {
			registry.close();
		}

	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testTimeGaugeShouldDropNanValue(RedisTestContext context) throws Exception {
		RedisTimeSeriesMeterRegistry registry = registry(context);
		try {
			AtomicReference<Double> obj = new AtomicReference<>(Double.NaN);
			registry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
			TimeGauge timeGauge = registry.get("my.time.gauge").timeGauge();
			registry.write(timeGauge);
			assertKeyAbsent(context, "my:time:gauge");
		} finally {
			registry.close();
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testTimeGaugeShouldDropInfiniteValues(RedisTestContext context) throws Exception {
		RedisTimeSeriesMeterRegistry registry = registry(context);
		try {
			AtomicReference<Double> obj = new AtomicReference<>(Double.POSITIVE_INFINITY);
			registry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
			TimeGauge timeGauge = registry.get("my.time.gauge").timeGauge();
			registry.write(timeGauge);
			assertKeyAbsent(context, "my:time:gauge");
			obj = new AtomicReference<>(Double.NEGATIVE_INFINITY);
			registry.more().timeGauge("my.time.gauge", Tags.empty(), obj, TimeUnit.SECONDS, AtomicReference::get);
			timeGauge = registry.get("my.time.gauge").timeGauge();
			registry.write(timeGauge);
			assertKeyAbsent(context, "my:time:gauge");
		} finally {
			registry.close();
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void longTaskTimer(RedisTestContext context) throws Exception {
		RedisTimeSeriesMeterRegistry registry = registry(context);
		try {
			LongTaskTimer timer = LongTaskTimer.builder("my.timer").tag("tag", "value").register(registry);
			registry.write(timer);
			List<GetResult<String, String>> results = context.sync().tsMget("tag=value");
			Assertions.assertEquals(3, results.size());
			Assertions.assertEquals("my:timer:active:count", results.get(0).getKey());
			Assertions.assertEquals("my:timer:duration:sum", results.get(1).getKey());
			Assertions.assertEquals("my:timer:max", results.get(2).getKey());
		} finally {
			registry.close();
		}
	}
}
