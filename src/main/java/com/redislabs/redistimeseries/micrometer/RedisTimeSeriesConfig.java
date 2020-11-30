package com.redislabs.redistimeseries.micrometer;

import static io.micrometer.core.instrument.config.MeterRegistryConfigValidator.checkAll;
import static io.micrometer.core.instrument.config.MeterRegistryConfigValidator.checkRequired;
import static io.micrometer.core.instrument.config.validate.PropertyValidator.getUrlString;
import static io.micrometer.core.instrument.config.validate.PropertyValidator.getInteger;

import io.lettuce.core.RedisURI;
import io.micrometer.core.instrument.config.validate.Validated;
import io.micrometer.core.instrument.step.StepRegistryConfig;

/**
 * Configuration for {@link RedisTimeSeriesMeterRegistry}.
 *
 * @author Julien Ruaux
 */
public interface RedisTimeSeriesConfig extends StepRegistryConfig {
	/**
	 * Accept configuration defaults
	 */
	RedisTimeSeriesConfig DEFAULT = k -> null;

	@Override
	default String prefix() {
		return "redistimeseries";
	}

	/**
	 * @return RedisTimeSeries connection string.
	 */
	default RedisURI uri() {
		return RedisURI.create(getUrlString(this, "uri").orElse("redis://localhost:6379"));
	}

	default int poolMaxTotal() {
		return getInteger(this, "poolMaxTotal").orElse(8);
	}

	@Override
	default Validated<?> validate() {
		return checkAll(this, c -> StepRegistryConfig.validate(c), checkRequired("uri", RedisTimeSeriesConfig::uri));
	}
}
