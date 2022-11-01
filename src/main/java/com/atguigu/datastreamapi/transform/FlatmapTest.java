package com.atguigu.datastreamapi.transform;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class FlatmapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickEventSource())
                .flatMap((Event value, Collector<String> out) -> {
                    out.collect(value.user);
                    out.collect(value.url);
                })
                .returns(Types.STRING)
//                .flatMap(new FlatMapFunction<Event, String>() {
//                    @Override
//                    public void flatMap(Event value, Collector<String> out) throws Exception {
//                        out.collect(value.user);
//                        out.collect(value.url);
//                    }
//                })
                .print();

        env.execute();
    }
}
