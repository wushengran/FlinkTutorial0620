package com.atguigu.datastreamapi.processfunction;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
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

public class KeyedProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp)));

        stream.print("input");

        stream.keyBy(value -> value.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        long watermark = ctx.timerService().currentWatermark();
                        long processingTime = ctx.timerService().currentProcessingTime();
                        out.collect("水位线为：" + watermark + "\n" +
                                "处理时间为：" + processingTime + "\n");

                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10000);
//                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 20000);
                        ctx.timerService().registerProcessingTimeTimer(ctx.timestamp() + 10000);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器触发！时间戳为：" + timestamp + "\n" +
                                "水位线为：" + ctx.timerService().currentWatermark() + "\n" +
                                "处理时间为：" + ctx.timerService().currentProcessingTime() + "\n");
                    }
                })
                .print();


        env.execute();
    }
}
