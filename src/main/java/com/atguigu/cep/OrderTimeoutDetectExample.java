package com.atguigu.cep;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class OrderTimeoutDetectExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<OrderEvent> orderStream = env.fromElements(
                        new OrderEvent("user_1", "order_1", "create", 1000L),
                        new OrderEvent("user_2", "order_2", "create", 2000L),
                        new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                        new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.ts))
                );

        // 1. 定义Pattern
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("create");
                    }
                })
                .followedBy("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return value.eventType.equals("pay");
                    }
                })
                .within(Time.minutes(15));

        // 2. 将模式应用在数据流上，检测匹配的事件
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderStream.keyBy(value -> value.orderId), pattern);

        // 3. 定义输出标签，用来表示超时未支付的复杂事件流
        OutputTag<String> outputTag = new OutputTag<String>("timeout") {};

        // 4. 基于PatternStream，提取匹配的事件和部分匹配的超时事件，输出
        SingleOutputStreamOperator<String> payStream = patternStream
                .sideOutputLateData(new OutputTag<OrderEvent>("late"))
                .process(new OrderPayTimeoutDetect());

        payStream.print("payed");
        payStream.getSideOutput(outputTag).print("timeout");

        env.execute();
    }

    // 实现自定义的PatternProcessFunction
    public static class OrderPayTimeoutDetect extends PatternProcessFunction<OrderEvent, String>
            implements TimedOutPartialMatchHandler<OrderEvent> {
        @Override
        public void processMatch(Map<String, List<OrderEvent>> match, Context ctx, Collector<String> out) throws Exception {
            OrderEvent createEvent = match.get("create").get(0);
            OrderEvent payEvent = match.get("pay").get(0);

            out.collect("订单成功支付！" + createEvent + "  " + payEvent);
        }

        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> match, Context ctx) throws Exception {
            OrderEvent createEvent = match.get("create").get(0);
            ctx.output(new OutputTag<String>("timeout") {}, "订单超时未支付！" + createEvent);
        }
    }
}
