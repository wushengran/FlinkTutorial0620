package com.atguigu.datastreamapi.processfunction;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

public class TopN_ProcessWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                );

        stream.print("input");

        // 统计每10秒中，热门url的TOP 2
        // 将所有数据发往同一个窗口，然后用全窗口函数统计每个url的个数，再排序
        stream.keyBy(value -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Event, String, Boolean, TimeWindow>() {
                    @Override
                    public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        // 定义一个HashMap，保存(url, count)
                        HashMap<String, Long> urlCountMap = new HashMap<>();

                        // 统计每个url的count数
                        for (Event element : elements) {
                            Long count = urlCountMap.getOrDefault(element.url, 0L);
                            urlCountMap.put(element.url, count + 1);
                        }

                        // 将（url, count）保存到列表，方便排序
                        ArrayList<Tuple2<String, Long>> urlCountList = new ArrayList<>();
                        for (String url : urlCountMap.keySet()) {
                            urlCountList.add(Tuple2.of(url, urlCountMap.get(url)));
                        }

                        // 排序
                        urlCountList.sort(new Comparator<Tuple2<String, Long>>() {
                            @Override
                            public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                                return Long.compare(o2.f1, o1.f1);
                            }
                        });

                        // 通用化的可视化结果显示
                        StringBuilder stringBuilder = new StringBuilder();
                        // 首先输出窗口信息
                        stringBuilder.append("窗口：[" + context.window().getStart() + " ~ " + context.window().getEnd() + ") \n");
                        // 遍历排序后的列表，取前n个
                        for (int i = 0; i < Math.min(2, urlCountList.size()); i++) {
                            Tuple2<String, Long> urlCount = urlCountList.get(i);
                            stringBuilder.append("NO" + (i + 1) + "  " +
                                    "URL: " + urlCount.f0 + "  " +
                                    "访问量：" + urlCount.f1 + "\n");
                        }
                        stringBuilder.append("-----------------------------\n");

                        out.collect(stringBuilder.toString());
                    }
                })
                .print();

        env.execute();
    }
}
