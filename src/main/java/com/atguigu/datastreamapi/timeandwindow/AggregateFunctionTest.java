package com.atguigu.datastreamapi.timeandwindow;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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

public class AggregateFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                );

        // 开窗计算，计算每10秒钟内每个用户的访问频次
        stream.keyBy(value -> value.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<Event, Tuple2<String, Integer>, String>() {
                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
                        return Tuple2.of("", 0);
                    }

                    @Override
                    public Tuple2<String, Integer> add(Event value, Tuple2<String, Integer> accumulator) {
                        return Tuple2.of(value.user, accumulator.f1 + 1);
                    }

                    @Override
                    public String getResult(Tuple2<String, Integer> accumulator) {
                        return "用户" + accumulator.f0 + "在10秒内的访问频次为：" + accumulator.f1;
                    }

                    @Override
                    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        return Tuple2.of(a.f0, a.f1 + b.f1);
                    }
                })
                .print();

        // 计算每个用户10秒内访问的平均时间戳
        stream.keyBy(value -> value.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate( new AvgTs() )
                .print();

        env.execute();
    }

    // 实现自定义的AggregateFunction
    public static class AvgTs implements AggregateFunction<Event, Tuple3<String, Long, Integer>, Tuple2<String, Long>>{
        @Override
        public Tuple3<String, Long, Integer> createAccumulator() {
            return Tuple3.of("", 0L, 0);
        }

        @Override
        public Tuple3<String, Long, Integer> add(Event value, Tuple3<String, Long, Integer> accumulator) {
            return Tuple3.of(value.user, accumulator.f1 + value.timestamp, accumulator.f2 + 1);
        }

        @Override
        public Tuple2<String, Long> getResult(Tuple3<String, Long, Integer> accumulator) {
            return Tuple2.of(accumulator.f0, accumulator.f1 / accumulator.f2) ;
        }

        @Override
        public Tuple3<String, Long, Integer> merge(Tuple3<String, Long, Integer> a, Tuple3<String, Long, Integer> b) {
            return null;
        }
    }
}
