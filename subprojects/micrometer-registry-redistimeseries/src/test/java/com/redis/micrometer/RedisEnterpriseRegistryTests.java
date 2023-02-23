package com.redis.micrometer;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.springframework.util.unit.DataSize;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisServer;

@EnabledOnOs(OS.LINUX)
class RedisEnterpriseRegistryTests extends BaseRegistryTests {

	private final RedisEnterpriseContainer container = new RedisEnterpriseContainer(
			RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag("latest"))
			.withDatabase(Database.name("MicrometerTests").memory(DataSize.ofMegabytes(90)).ossCluster(true)
					.modules(RedisModule.TIMESERIES).build());

	@Override
	protected RedisServer getRedisServer() {
		return container;
	}

}
