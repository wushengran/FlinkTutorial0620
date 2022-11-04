package com.atguigu.datastreamapi.timeandwindow;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class UrlViewCountExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
//                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
//                );

        SingleOutputStreamOperator<Event> stream = env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] fields = value.split(",");
                    return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                );

        stream.print("input");

        // 定义一个输出标签，用来表示迟到数据流
        OutputTag<Event> outputTag = new OutputTag<Event>("late") {
        };

        // 开窗计算，计算每10秒钟内每个页面url的访问频次
        SingleOutputStreamOperator<UrlViewCount> countStream = stream.keyBy(value -> value.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1))    // 设置窗口的延迟时间
                .sideOutputLateData(outputTag)    // 输出迟到数据到侧输出流
                .aggregate(new UrlCountAgg(), new UrlCountWindowResult());

        countStream.print("count");
        countStream.getSideOutput(outputTag).print("late");

        // 统计最热门的页面
//        countStream.keyBy(value -> value.window_start)
//                .max("count")
//                .print("max");

        env.execute();
    }

    // 实现自定义的AggregateFunction
    public static class UrlCountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        // 每来一条数据，计数器加1
        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 实现自定义的ProcessWindowFunction
    public static class UrlCountWindowResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String url, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            Long count = elements.iterator().next();
            Long start = context.window().getStart();
            Long end = context.window().getEnd();

            out.collect(new UrlViewCount(url, count, start, end));
        }
    }
}
