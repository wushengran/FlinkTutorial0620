package com.atguigu.datastreamapi.sink;

import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./fav", 3000L),
                new Event("Cary", "./home", 4000L),
                new Event("Cary", "./prod?id=1", 5000L),
                new Event("Alice", "./home", 6000L),
                new Event("Alice", "./prod?id=100", 7000L),
                new Event("Mary", "./prod?id=1", 8000L),
                new Event("Mary", "./prod?id=100", 9000L)
        );

        stream.map(Event::toString)
                .addSink(
                        StreamingFileSink
                                .<String>forRowFormat(new Path("output"), new SimpleStringEncoder<>())
                                .withRollingPolicy(
                                        DefaultRollingPolicy.builder()
                                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                                .withMaxPartSize(1024 * 1024 * 1024)
                                                .build()
                                )
                                .build()
                );

        env.execute();
    }
}
