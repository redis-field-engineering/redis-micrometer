package com.redis.micrometer;

import io.micrometer.core.instrument.config.validate.PropertyValidator;
import io.micrometer.core.instrument.step.StepRegistryConfig;

/**
 * {@link StepRegistryConfig} for Redis.
 *
 * @author Julien Ruaux
 */
public interface RedisRegistryConfig extends StepRegistryConfig {

	RedisRegistryConfig DEFAULT = k -> null;

	String DEFAULT_PREFIX = "redis";

	String DEFAULT_KEY_SEPARATOR = ":";

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

	/**
	 * 
	 * @return prefix for Redis keys created by the registry, for example "metrics"
	 *         (default: none).
	 */
	default String keyspace() {
		return PropertyValidator.getString(this, "keyspace").orElse(null);
	}

	default String keySeparator() {
		return PropertyValidator.getString(this, "keySeparator").orElse(DEFAULT_KEY_SEPARATOR);
	}

}
