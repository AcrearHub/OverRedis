package com.atguigu;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;

public class JedisTest {
    private static final String HOST = "hadoop102";
    private static final int PROT = 6379;
    public static void main(String[] args) {
//        Jedis jedis = getJedis();
        Jedis jedisFromPool = getJedisFromPool();
        String ping = jedisFromPool.ping();
        System.out.println(ping);
        testString();
        testList();
        testSet();
        testZSet();
        testHash();
        jedisFromPool.close();
    }

    /**
     * 直接连接
     */
    public static Jedis getJedis(){
        Jedis jedis = new Jedis(HOST,PROT);
        return jedis;
    }

    /**
     * 借助连接池进行连接
     */
    private static JedisPool jedisPool;
    public static Jedis getJedisFromPool(){
        if (jedisPool==null){
            //配置连接池配置
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(10);//最大连接数
            jedisPoolConfig.setMaxTotal(5);//空闲最大连接数
            jedisPoolConfig.setMinIdle(5);//空闲最小连接数
            jedisPoolConfig.setBlockWhenExhausted(false);//连接耗尽时是否等待
            jedisPoolConfig.setMaxWaitMillis(2000);//最长等待时间;
            jedisPoolConfig.setTestOnBorrow(true);//对连接是否进行测试
            //创建连接池
            jedisPool = new JedisPool(jedisPoolConfig,HOST, PROT);
        }
        return jedisPool.getResource();
    }

    /**
     * 测试五大数据类型
     */
    public static void testString(){
        Jedis jedisFromPool = getJedisFromPool();

        System.out.println("========String========");
        jedisFromPool.set("k1","v1");
        System.out.println(jedisFromPool.get("k1"));
        jedisFromPool.append("k1","v2");
        System.out.println(jedisFromPool.get("k1"));
        System.out.println(jedisFromPool.strlen("k1"));
        jedisFromPool.incrBy("k2",5);
        System.out.println(jedisFromPool.get("k2"));
        jedisFromPool.set("k3","lishipeng");
        System.out.println(jedisFromPool.getrange("k3",0,4));

        jedisFromPool.close();
    }
    public static void testList(){
        Jedis jedisFromPool = getJedisFromPool();

        System.out.println("========List========");
        jedisFromPool.lpush("k4","1","2","3","4","5");
        System.out.println(jedisFromPool.lrange("k4",0,-1));
        jedisFromPool.lpop("k4");
        System.out.println(jedisFromPool.lrange("k4",0,-1));
        jedisFromPool.lrem("k4",1,"1");
        System.out.println(jedisFromPool.lrange("k4",0,-1));
        jedisFromPool.del("k4");

        jedisFromPool.close();
    }
    public static void testSet(){
        Jedis jedisFromPool = getJedisFromPool();

        System.out.println("========Set========");
        jedisFromPool.sadd("k5","1","2","3","4","5");
        System.out.println(jedisFromPool.smembers("k5"));
        System.out.println(jedisFromPool.sismember("k5","4"));
        System.out.println(jedisFromPool.scard("k5"));
        jedisFromPool.spop("k5");
        System.out.println(jedisFromPool.smembers("k5"));
        jedisFromPool.sadd("k6","4","5","6","7","8");
        System.out.println(jedisFromPool.sdiff("k5","k6"));

        jedisFromPool.close();
    }
    public static void testZSet(){
        Jedis jedisFromPool = getJedisFromPool();

        System.out.println("========ZSet========");
        HashMap<String, Double> value = new HashMap<>();
        value.put("Java",100.0);
        value.put("Scala",80.0);
        value.put("Python",90.0);
        jedisFromPool.zadd("k7",value);
        System.out.println(jedisFromPool.zrangeWithScores("k7",0,-1));
        System.out.println(jedisFromPool.zrevrange("k7",0,-1));
        System.out.println(jedisFromPool.zcount("k7",80,90));
        System.out.println(jedisFromPool.zrank("k7","Python"));

        jedisFromPool.close();
    }
    public static void testHash(){
        Jedis jedisFromPool = getJedisFromPool();

        System.out.println("========Hash========");
        HashMap<String, String> value = new HashMap<>();
        value.put("name","zhangsan");
        value.put("age","23");
        value.put("sex","male");
        jedisFromPool.hset("k8",value);
        System.out.println(jedisFromPool.hkeys("k8"));
        System.out.println(jedisFromPool.hvals("k8"));
        System.out.println(jedisFromPool.hget("k8","name"));

        jedisFromPool.close();
    }
}
