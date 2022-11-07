package com.atguigu.datastreamapi.state;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class PeriodicCountExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                );

        stream.print("input");

        // 使用定时器隔一段时间触发一次统计结果的输出，统计每个用户的活跃度
        stream.keyBy(value -> value.user)
                .process(new PeriodicCount())
                .print();

        env.execute();
    }

    // 实现自定义的处理函数
    public static class PeriodicCount extends KeyedProcessFunction<String, Event, Tuple2<String, Long>>{
        // 使用值状态记录当前访问量
        ValueState<Long> countState;
        // 使用值状态记录是否注册过定时器，保存定时器时间戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("user-count", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, Tuple2<String, Long>>.Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            // 来一条数据，就加1
            if (countState.value() == null)
                countState.update(1L);
            else
                countState.update(countState.value() + 1 );

            // 注册定时器，5秒后触发输出结果
            if (timerTsState.value() == null){
                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000);
                timerTsState.update(ctx.timestamp() + 5000);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, Tuple2<String, Long>>.OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            // 定时器触发，输出结果
            out.collect( Tuple2.of(ctx.getCurrentKey(), countState.value()) );

            // 清空状态
            timerTsState.clear();
//            ctx.timerService().registerEventTimeTimer(timestamp + 5000);
//            timerTsState.update(timestamp + 5000);
        }
    }
}
