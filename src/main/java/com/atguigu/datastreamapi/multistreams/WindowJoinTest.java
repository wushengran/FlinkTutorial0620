package com.atguigu.datastreamapi.multistreams;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class WindowJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //  读取两条流
        SingleOutputStreamOperator<Event> stream1 = env.fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 2000L),
                        new Event("Alice", "./fav", 3000L),
                        new Event("Cary", "./home", 4000L),
                        new Event("Cary", "./prod?id=1", 5000L),
                        new Event("Alice", "./home", 6000L),
                        new Event("Alice", "./prod?id=100", 17000L),
                        new Event("Mary", "./prod?id=1", 18000L),
                        new Event("Mary", "./prod?id=100", 19000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                );

        SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = env.fromElements(
                Tuple2.of("Alice", 1000L),
                Tuple2.of("Bob", 2000L),
                Tuple2.of("Alice", 5000L),
                Tuple2.of("Alice", 10000L),
                Tuple2.of("Alice", 12000L),
                Tuple2.of("Bob", 15000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner(((element, recordTimestamp) -> element.f1))
        );

        // 窗口联结，10秒滚动窗口
        stream1.join(stream2)
                .where(value -> value.user)
                .equalTo(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Event, Tuple2<String, Long>, String>() {
                    @Override
                    public String join(Event first, Tuple2<String, Long> second) throws Exception {
                        return first + " -> " + second;
                    }
                })
                .print();

        env.execute();
    }
}
