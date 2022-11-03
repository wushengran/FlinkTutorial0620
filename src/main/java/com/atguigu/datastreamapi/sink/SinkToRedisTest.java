package com.atguigu.datastreamapi.sink;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class SinkToRedisTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<Event> stream = env.fromElements(
//                new Event("Alice", "./home", 1000L),
//                new Event("Bob", "./cart", 2000L),
//                new Event("Alice", "./fav", 3000L),
//                new Event("Cary", "./home", 4000L),
//                new Event("Cary", "./prod?id=1", 5000L),
//                new Event("Alice", "./home", 6000L),
//                new Event("Alice", "./prod?id=100", 7000L),
//                new Event("Mary", "./prod?id=1", 8000L),
//                new Event("Mary", "./prod?id=100", 9000L)
//        );

        DataStreamSource<Event> stream = env.addSource(new ClickEventSource());

        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();

        RedisMapper<Event> redisMapper = new RedisMapper<Event>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "clicks");
            }

            @Override
            public String getKeyFromData(Event data) {
                return data.user;
            }

            @Override
            public String getValueFromData(Event data) {
                return data.url;
            }
        };

        // 将 (user, url) 存入redis
        stream.addSink( new RedisSink<>(jedisPoolConfig, redisMapper));

        env.execute();
    }
}
