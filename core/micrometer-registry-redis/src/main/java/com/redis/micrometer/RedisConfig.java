package com.redis.micrometer;

import java.util.Collections;
import java.util.Set;

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

	Set<String> EMPTY_EXCLUDED_TAGS = Collections.emptySet();

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

	/**
	 * 
	 * @return tag names to exclude from key building
	 */
	default Set<String> ignoreKeyTags() {
		return EMPTY_EXCLUDED_TAGS;
	}

}
