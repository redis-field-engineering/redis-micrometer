package com.redis.micrometer;

import io.micrometer.core.instrument.config.validate.PropertyValidator;
import io.micrometer.core.instrument.step.StepRegistryConfig;

/**
 * {@link StepRegistryConfig} for Redis.
 *
 * @author Julien Ruaux
 */
public interface RedisConfig extends StepRegistryConfig {

	String DEFAULT_PREFIX = "redis";

	RedisConfig DEFAULT = k -> null;

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
	 * @return prefix for Redis keys created by the registry, for example "metrics:"
	 *         (default: none).
	 */
	default String keyPrefix() {
		return PropertyValidator.getString(this, "keyPrefix").orElse("");
	}

	default String keySeparator() {
		return PropertyValidator.getString(this, "keySeparator").orElse(DEFAULT_KEY_SEPARATOR);
	}

}
