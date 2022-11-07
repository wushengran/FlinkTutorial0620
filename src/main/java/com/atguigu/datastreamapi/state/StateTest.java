package com.atguigu.datastreamapi.state;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
                    private long n = 0L;
                    ValueState<Long> myValueState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        myValueState = getRuntimeContext().getState( new ValueStateDescriptor<Long>("my-value", Long.class));
                    }

                    @Override
                    public void flatMap(Event value, Collector<String> out) throws Exception {
                        // 值状态
                        if (myValueState.value() == null)
                            myValueState.update(1L);
                        else
                            myValueState.update(myValueState.value() + 1);

                        out.collect("用户" + value.user + " value state: " + myValueState.value() + "\n");

                        // 做对比，考察一个本地变量
                        out.collect( "n: " + ++n + "\n");
                    }
                })
                .print();

        env.execute();
    }
}
