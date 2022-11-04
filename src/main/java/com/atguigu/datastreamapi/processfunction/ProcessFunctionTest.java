package com.atguigu.datastreamapi.processfunction;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp)));

        OutputTag<Event> outputTag = new OutputTag<Event>("side") {
        };

        SingleOutputStreamOperator<String> result = stream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value.user);
                out.collect(value.url);
                out.collect("WM: " + ctx.timerService().currentWatermark());
                Long timestamp = ctx.timestamp();
//                ctx.timerService().registerEventTimeTimer(timestamp + 10000);
                ctx.output(outputTag, value);
            }

//            @Override
//            public void onTimer(long timestamp, ProcessFunction<Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
//                super.onTimer(timestamp, ctx, out);
//            }
        });

        result.print("result");
        result.getSideOutput(outputTag).print("side");

        env.execute();
    }
}
