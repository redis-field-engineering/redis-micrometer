package com.redis.micrometer;

import org.testcontainers.junit.jupiter.Container;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

public class RedisStackRegistryTests extends BaseRegistryTests {

	@Container
	private static final RedisStackContainer redisStack = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	@Override
	RedisServer getRedisServer() {
		return redisStack;
	}

}
