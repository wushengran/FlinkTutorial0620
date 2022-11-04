package com.atguigu.datastreamapi.timeandwindow;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class WindowFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                );

        // 开窗计算，计算每10秒钟内每个用户的访问频次
        stream.keyBy(value -> value.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<Event, String, String, TimeWindow>() {
                    @Override
                    public void apply(String user, TimeWindow window, Iterable<Event> input, Collector<String> out) throws Exception {
                        long start = window.getStart();
                        long end = window.getEnd();
                        long count = 0L;
                        for (Event event : input) {
                            count ++;
                        }

                        out.collect( "窗口 [" + start + " ~ " + end + ")  用户：" + user + "  访问频次：" + count);
                    }
                })
                .print();

        env.execute();
    }
}
