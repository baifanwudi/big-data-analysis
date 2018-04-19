package com.adups.test.redis;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
/**
 * @author allen
 * @date 26/12/2017.
 */
public class RedisPool {

	private static final Logger logger = LoggerFactory.getLogger(RedisPool.class);

	private JedisPool jedisPool;

	private JedisPoolConfig jedisPoolConfig;

	public RedisPool() {

			jedisPoolConfig=new JedisPoolConfig();
			jedisPoolConfig.setMaxTotal(1024);
			jedisPoolConfig.setMaxIdle(200);
			jedisPoolConfig.setMaxWaitMillis(10000);
			jedisPoolConfig.setTestOnBorrow(true);

	}

	public  JedisPool getInstance(){
		if(jedisPool==null){
			synchronized (this){
				if(jedisPool==null){
					jedisPool= new JedisPool(jedisPoolConfig,"localhost",6379,10000);
				}
			}
		}
		return jedisPool;
	}

}
