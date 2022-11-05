package com.atguigu.datastreamapi.multistreams;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.stream.Stream;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class SplitStreamTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                );

        // 1. 使用filter实现分流
        SingleOutputStreamOperator<Event> aliceStream = stream.filter(value -> value.user.equals("Alice"));
        SingleOutputStreamOperator<Event> bobStream = stream.filter(value -> value.user.equals("Bob"));
        SingleOutputStreamOperator<Event> elseStream = stream.filter(value -> !value.user.equals("Alice") && !value.user.equals("Bob"));

//        aliceStream.print("alice");
//        bobStream.print("bob");
//        elseStream.print("else");

        // 2. 使用侧输出流
        OutputTag<Event> aliceTag = new OutputTag<Event>("alice") {};
        OutputTag<Tuple3<String, String, Long>> bobTag = new OutputTag<Tuple3<String, String, Long>>("bob"){};

        SingleOutputStreamOperator<Event> resultStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                if (value.user.equals("Alice"))
                    ctx.output(aliceTag, value);
                else if (value.user.equals("Bob"))
                    ctx.output(bobTag, Tuple3.of(value.user, value.url, value.timestamp));
                else
                    out.collect(value);
            }
        });

        resultStream.getSideOutput(aliceTag).print("alice");
        resultStream.getSideOutput(bobTag).print("bob");
        resultStream.print("else");

        env.execute();
    }
}
