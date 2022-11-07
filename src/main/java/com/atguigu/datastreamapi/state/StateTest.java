package com.atguigu.datastreamapi.state;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class StateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp)
                        )
                );

        stream.print("input");

        // 统计每个用户的访问频次
        stream.keyBy(value -> value.user)
                .flatMap(new RichFlatMapFunction<Event, String>() {
                    // 定义属性
                    private long n = 0L;
                    // 定义状态
                    ValueState<Long> myValueState;
                    ListState<Event> myListState;
                    MapState<String, Long> myMapState;
                    ReducingState<Event> myReducingState;
                    AggregatingState<Event, String> myAggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        myValueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("my-value", Long.class));
                        myListState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("my-list", Event.class));
                        myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("my-map", String.class, Long.class));
                        myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>(
                                "my-reducing",
                                (value1, value2) -> new Event(value1.user, value1.url + ", " + value2.url, value2.timestamp),
                                Event.class
                        ));
                        myAggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>(
                                "my-aggregating",
                                new AggregateFunction<Event, Long, String>() {
                                    @Override
                                    public Long createAccumulator() {
                                        return 0L;
                                    }

                                    @Override
                                    public Long add(Event value, Long accumulator) {
                                        return accumulator + 1;
                                    }

                                    @Override
                                    public String getResult(Long accumulator) {
                                        return " aggregating state: " + accumulator;
                                    }

                                    @Override
                                    public Long merge(Long a, Long b) {
                                        return null;
                                    }
                                },
                                Long.class
                        ));
                    }

                    @Override
                    public void flatMap(Event value, Collector<String> out) throws Exception {
                        // 值状态
                        if (myValueState.value() == null)
                            myValueState.update(1L);
                        else
                            myValueState.update(myValueState.value() + 1);

                        out.collect("用户" + value.user + " value state: " + myValueState.value());

                        // 列表状态
                        myListState.add(value);
                        out.collect("用户" + value.user + " list state: " + myListState.get());

                        // 映射状态
                        // 统计当前用户，对每个页面的访问量
                        if (myMapState.contains(value.url))
                            myMapState.put(value.url, myMapState.get(value.url) + 1);
                        else
                            myMapState.put(value.url, 1L);
                        out.collect("用户" + value.user + " url: " + value.url + " 访问量：" + myMapState.get(value.url));

                        // 归约状态
                        myReducingState.add(value);
                        out.collect( "用户" + value.user + " Reducing State: " + myReducingState.get() );

                        // 聚合状态
                        myAggregatingState.add(value);
                        out.collect( "用户" + value.user + myAggregatingState.get() );

                        // 做对比，考察一个本地变量
                        out.collect("n: " + ++n + "\n");
                    }
                })
                .print();

        env.execute();
    }
}
