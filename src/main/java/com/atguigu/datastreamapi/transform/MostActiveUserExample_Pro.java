package com.atguigu.datastreamapi.transform;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class MostActiveUserExample_Pro {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickEventSource());
        stream.print("input");

        // 1. 按用户分组，统计每个用户的访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> userCountStream = stream.map(value -> Tuple2.of(value.user, 1L))
                .returns(new TypeHint<Tuple2<String, Long>>() {
                })
                .keyBy(value -> value.f0)
                .reduce(((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1)));
//                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
//                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
//                    }
//                });
        userCountStream.print("count");

        // 2. 统计所有用户中访问频次最大的
        SingleOutputStreamOperator<Tuple2<String, Long>> result = userCountStream.keyBy(value -> "1")
                .reduce( ((value1, value2) -> {
                    return value1.f1 > value2.f1 ? value1 : value2;
                }));

        result.print("max");

        env.execute();
    }
}
