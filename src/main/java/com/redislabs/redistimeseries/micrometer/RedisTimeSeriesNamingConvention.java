package com.redislabs.redistimeseries.micrometer;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.lang.Nullable;

/**
 * {@link NamingConvention} for RedisTimeSeries.
 *
 * @author Julien Ruaux
 */
public class RedisTimeSeriesNamingConvention implements NamingConvention {

	@Override
	public String name(String name, Meter.Type type, @Nullable String baseUnit) {
		return toColonDelimited(name);
	}

	@Override
	public String tagKey(String key) {
		return toColonDelimited(key);
	}

	private String toColonDelimited(String value) {
		return Arrays.stream(value.split("\\.")).filter(Objects::nonNull).collect(Collectors.joining(":"));
	}
}
