package com.atguigu.datastreamapi.state;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import com.atguigu.datastreamapi.timeandwindow.UrlViewCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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

public class SimulatedWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                );

        stream.print("input");

        // 使用定时器模拟窗口的触发，统计每10秒钟每个页面的访问量
        stream.keyBy(value -> value.url)
                .process(new SimulatedTumblingWindow(10000L))
                .print();

        env.execute();
    }

    // 实现自定义的处理函数
    public static class SimulatedTumblingWindow extends KeyedProcessFunction<String, Event, UrlViewCount>{
        private Long size;

        public SimulatedTumblingWindow(Long size) {
            this.size = size;
        }

        // 使用MapState保存每个窗口内的访问值，（window_start, count）
        MapState<Long, Long> countPerWindowMapState;


        @Override
        public void open(Configuration parameters) throws Exception {
            countPerWindowMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("count-per-window", Long.class, Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, UrlViewCount>.Context ctx, Collector<UrlViewCount> out) throws Exception {
            // 根据数据的时间戳确定所属的窗口
            Long start = value.timestamp - value.timestamp % size ;
            Long end = start + size;

            // 按照窗口统计访问频次，增量聚合
            if (countPerWindowMapState.contains(start))
                countPerWindowMapState.put( start, countPerWindowMapState.get(start) + 1 );
            else
                countPerWindowMapState.put( start, 1L );

            // 按照窗口结束时间注册定时器
            ctx.timerService().registerEventTimeTimer( end - 1 );
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, UrlViewCount>.OnTimerContext ctx, Collector<UrlViewCount> out) throws Exception {
            // 定时器触发，执行窗口计算和输出
            String url = ctx.getCurrentKey();
            Long end = timestamp + 1;
            Long start = end - size;
            Long count = countPerWindowMapState.get(start);
            out.collect(new UrlViewCount(url, count, start, end));

            // 关闭窗口
            countPerWindowMapState.remove(start);
        }
    }
}
