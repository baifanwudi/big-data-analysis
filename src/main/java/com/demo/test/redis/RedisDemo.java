package com.demo.test.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


/**
 * @author allen
 * @date 26/12/2017.
 */
public class RedisDemo {

	public static void main(String[] args) {

		JedisPoolConfig jedisPoolConfig=new JedisPoolConfig();
		jedisPoolConfig.setMaxTotal(1024);
		jedisPoolConfig.setMaxIdle(200);
		jedisPoolConfig.setMaxWaitMillis(10000);
		jedisPoolConfig.setTestOnBorrow(true);

		JedisPool jedisPool=new JedisPool(jedisPoolConfig,"localhost",6379,10000);

		Jedis jedis=jedisPool.getResource();
//
//		jedis.set("bai","test");
//		jedis.expire("bai",10);
//		jedis.setex("allen",3600,"test");
//		System.out.println(jedis.get("allen"));
		System.out.println(jedis.keys("alle"));

	}
}
