package com.atguigu.datastreamapi.transform;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class FilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickEventSource())
                .filter( value -> value.user.equals("Alice") )
//                .filter(new FilterFunction<Event>() {
//                    @Override
//                    public boolean filter(Event value) throws Exception {
//                        return value.user.equals("Alice");
//                    }
//                })
                .print();

        env.execute();
    }
}
