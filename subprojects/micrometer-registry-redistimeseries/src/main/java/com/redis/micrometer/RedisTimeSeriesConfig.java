package com.redis.micrometer;

import static io.micrometer.core.instrument.config.MeterRegistryConfigValidator.checkAll;
import static io.micrometer.core.instrument.config.MeterRegistryConfigValidator.checkRequired;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import io.micrometer.core.instrument.config.validate.PropertyValidator;
import io.micrometer.core.instrument.config.validate.Validated;
import io.micrometer.core.instrument.step.StepRegistryConfig;

/**
 * {@link StepRegistryConfig} for RedisTimeSeries.
 *
 * @author Julien Ruaux
 */
public interface RedisTimeSeriesConfig extends StepRegistryConfig {

	RedisTimeSeriesConfig DEFAULT = k -> null;

	@Override
	default String prefix() {
		return "redistimeseries";
	}

	default String uri() {
		return PropertyValidator.getString(this, "uri").required().get();
	}

	default boolean cluster() {
		return PropertyValidator.getBoolean(this, "cluster").orElse(false);
	}

	default int poolMaxTotal() {
		return PropertyValidator.getInteger(this, "poolMaxTotal").orElse(GenericObjectPoolConfig.DEFAULT_MAX_TOTAL);
	}

	default String keyspace() {
		return PropertyValidator.getString(this, "keyspace").orElse(null);
	}

	@Override
	default Validated<?> validate() {
		return checkAll(this, c -> StepRegistryConfig.validate(c), checkRequired("uri", RedisTimeSeriesConfig::uri));
	}

}
