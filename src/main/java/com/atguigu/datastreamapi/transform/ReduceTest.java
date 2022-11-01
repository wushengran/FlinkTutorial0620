package com.atguigu.datastreamapi.transform;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class ReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 得到每个用户，最近一次的访问时间戳，以及目前访问过的所有url
        env.addSource(new ClickEventSource())
                .keyBy( value -> value.user )
                .reduce(new ReduceFunction<Event>() {
                    @Override
                    public Event reduce(Event value1, Event value2) throws Exception {
                        return new Event(value1.user, value1.url + " - " + value2.url, value2.timestamp);
                    }
                })
                .print();

        env.execute();
    }
}
