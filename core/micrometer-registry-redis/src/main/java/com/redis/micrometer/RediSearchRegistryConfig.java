package com.redis.micrometer;

import static io.micrometer.core.instrument.config.validate.PropertyValidator.getEnum;

import io.micrometer.core.instrument.config.validate.PropertyValidator;
import io.micrometer.core.instrument.simple.CountingMode;

public interface RediSearchRegistryConfig extends RedisRegistryConfig {

	String DEFAULT_INDEX_SEPARATOR = "-";

	String[] EMPTY_EXCLUDED_TAGS = new String[0];

	default String indexPrefix() {
		return PropertyValidator.getString(this, "indexPrefix").orElse(null);
	}

	default String indexSuffix() {
		return PropertyValidator.getString(this, "indexSuffix").orElse(null);
	}

	default String indexSeparator() {
		return PropertyValidator.getString(this, "indexNameSeparator").orElse(DEFAULT_INDEX_SEPARATOR);
	}

	/**
	 * 
	 * @return tag names to exclude from key building
	 */
	default String[] nonKeyTags() {
		return EMPTY_EXCLUDED_TAGS;
	}

	/**
	 * @return A mode that determines whether the registry reports cumulative values
	 *         over all time or a rate normalized form representing changes in the
	 *         last {@link #step()}.
	 */
	default CountingMode mode() {
		return getEnum(this, CountingMode.class, "mode").orElse(CountingMode.CUMULATIVE);
	}

}
