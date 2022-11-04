package com.atguigu.datastreamapi.timeandwindow;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
//        SingleOutputStreamOperator<Event> stream = env.fromElements(
//                        new Event("Alice", "./home", 1000L),
//                        new Event("Bob", "./cart", 2000L),
//                        new Event("Alice", "./fav", 3000L),
//                        new Event("Cary", "./home", 4000L),
//                        new Event("Cary", "./prod?id=1", 5000L),
//                        new Event("Alice", "./home", 6000L),
//                        new Event("Alice", "./prod?id=100", 17000L),
//                        new Event("Mary", "./prod?id=1", 18000L),
//                        new Event("Mary", "./prod?id=100", 19000L)
//                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                );

        // 开窗计算，计算每10秒钟内每个用户的访问频次
        stream.keyBy(value -> value.user)
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
//                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .countWindow(10, 2)
//                .evictor()
//                .trigger()
                .max("timestamp")
                .print();
        stream.map( value -> Tuple2.of(value.user, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy( value -> value.f0 )
                .window( TumblingEventTimeWindows.of(Time.seconds(10)) )
                .sum( 1 )
                .print();

        env.execute();
    }
}
