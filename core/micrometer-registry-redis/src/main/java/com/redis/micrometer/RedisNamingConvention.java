package com.redis.micrometer;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.NamingConvention;

/**
 * {@link NamingConvention} for Redis.
 *
 * Names and tags are mapped to Redis keys.
 *
 * @author Julien Ruaux
 */
public class RedisNamingConvention implements NamingConvention {

	private final String separator;

	public RedisNamingConvention(String separator) {
		this.separator = separator;
	}

	@Override
	public String name(String name, Meter.Type type, String baseUnit) {
		return toColonDelimited(name);
	}

	@Override
	public String tagKey(String key) {
		return toColonDelimited(key);
	}

	private String toColonDelimited(String value) {
		return Arrays.stream(value.split("\\.")).filter(Objects::nonNull).collect(Collectors.joining(separator));
	}

}
