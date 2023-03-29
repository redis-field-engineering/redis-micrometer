package com.redis.micrometer;

import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

class RedisStackRegistryTests extends AbstractRegistryTests {

	private static final RedisStackContainer REDIS_STACK = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	@Override
	protected RedisServer getRedisServer() {
		return REDIS_STACK;
	}

}
