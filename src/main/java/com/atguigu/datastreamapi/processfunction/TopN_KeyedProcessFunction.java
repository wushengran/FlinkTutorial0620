package com.atguigu.datastreamapi.processfunction;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import com.atguigu.datastreamapi.timeandwindow.UrlViewCount;
import com.atguigu.datastreamapi.timeandwindow.UrlViewCountExample;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class TopN_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                );

        stream.print("input");

        // 统计每10秒中，每个url的访问次数
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(value -> value.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UrlViewCountExample.UrlCountAgg(), new UrlViewCountExample.UrlCountWindowResult());

        urlCountStream.print("count");

        // 针对每个窗口，收集所有数据，排序输出Top N
        urlCountStream.keyBy(value -> value.window_end)
                .process(new TopNUrlCount(5))
                .print();

        env.execute();
    }

    // 实现自定义的处理函数
    public static class TopNUrlCount extends KeyedProcessFunction<Long, UrlViewCount, String>{
        private int n;
        // 声明列表状态
        private ListState<UrlViewCount> urlViewCountListState;

        public TopNUrlCount(int n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从运行时上下文中获取状态
            urlViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("urlViewCount-list", UrlViewCount.class));
        }

        @Override
        public void processElement(UrlViewCount value, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx, Collector<String> out) throws Exception {
            urlViewCountListState.add(value);
            // 注册一个窗口结束时间加1ms的定时器
            ctx.timerService().registerEventTimeTimer( ctx.getCurrentKey() + 1 );
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发时，收集到了所有数据，进行排序
            ArrayList<UrlViewCount> urlViewCounts = new ArrayList<>();
            for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
                urlViewCounts.add(urlViewCount);
            }

            urlViewCounts.sort( (value1, value2) -> Long.compare(value2.count, value1.count) );

            // 通用化的可视化结果显示
            // 首先输出窗口信息
            Long end = ctx.getCurrentKey();
            Long start = end - 10000;
            String result = "--------------------------------\n";
            result = result + "窗口：[" + start+ " ~ " + end + ") \n";
            // 遍历排序后的列表，取前n个
            for (int i = 0; i < Math.min(n, urlViewCounts.size()); i++) {
                UrlViewCount urlCount = urlViewCounts.get(i);
                result = result + ("NO" + (i + 1) + "  " +
                        "URL: " + urlCount.url + "  " +
                        "访问量：" + urlCount.count + "\n");
            }
            result = result + ("-----------------------------\n");

            out.collect(result);
        }
    }
}
