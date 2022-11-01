package com.atguigu.datastreamapi.transform;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class SimpleAggregateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Event> stream = env.addSource(new ClickEventSource());
        stream
//                .map( value -> Tuple2.of(value.user, value.timestamp))
//                .returns(new TypeHint<Tuple2<String, Long>>() {})
                .keyBy( value -> value.user )
                .maxBy("timestamp")
                .print("max");

        stream.print("input");

        env.execute();
    }
}
