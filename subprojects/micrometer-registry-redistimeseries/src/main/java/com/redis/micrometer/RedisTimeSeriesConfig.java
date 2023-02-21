package com.redis.micrometer;

import io.micrometer.core.instrument.config.validate.PropertyValidator;
import io.micrometer.core.instrument.step.StepRegistryConfig;

/**
 * {@link StepRegistryConfig} for RedisTimeSeries.
 *
 * @author Julien Ruaux
 */
public interface RedisTimeSeriesConfig extends StepRegistryConfig {

	String DEFAULT_PREFIX = "redis";

	RedisTimeSeriesConfig DEFAULT = k -> null;

	@Override
	default String prefix() {
		return DEFAULT_PREFIX;
	}

	default String uri() {
		return PropertyValidator.getString(this, "uri").required().get();
	}

	default boolean cluster() {
		return PropertyValidator.getBoolean(this, "cluster").orElse(false);
	}

	default String keyspace() {
		return PropertyValidator.getString(this, "keyspace").orElse(null);
	}

}
