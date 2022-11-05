package com.atguigu.datastreamapi.multistreams;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.lang.model.element.ElementVisitor;
import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class BillCheckExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取两条流数据
        SingleOutputStreamOperator<OrderEvent> orderStream = env.fromElements(
                        new OrderEvent("0001", "create", 1000L),
                        new OrderEvent("0002", "create", 2000L),
                        new OrderEvent("0003", "create", 3000L),
                        new OrderEvent("0001", "pay", 4000L),
                        new OrderEvent("0003", "pay", 5000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
                )
                .filter( value -> value.eventType.equals("pay") );

        SingleOutputStreamOperator<ThirdPartyEvent> thirdPartyStream = env.fromElements(
                        new ThirdPartyEvent("0001", "pay", "wechat", 5000L),
                        new ThirdPartyEvent("0004", "pay", "alipay", 7000L)
                        )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<ThirdPartyEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                );

        OutputTag<String> outputTag = new OutputTag<String>("warning") {};

        // 按照order id连接两条流
        SingleOutputStreamOperator<String> matchedStream = orderStream.connect(thirdPartyStream)
                .keyBy(value -> value.orderId, value -> value.orderId)
                .process(new OrderPayMatch());

        matchedStream.print("matched");
        matchedStream.getSideOutput(outputTag).print("warning");

        env.execute();
    }

    // 实现自定义的处理函数
    public static class OrderPayMatch extends CoProcessFunction<OrderEvent, ThirdPartyEvent, String>{
        // 定义值状态，保存已经到来的事件
        private ValueState<OrderEvent> orderEventState;
        private ValueState<ThirdPartyEvent> thirdPartyEventState;

        @Override
        public void open(Configuration parameters) throws Exception {
            orderEventState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order-event", OrderEvent.class));
            thirdPartyEventState = getRuntimeContext().getState(new ValueStateDescriptor<ThirdPartyEvent>("thirdparty-event", ThirdPartyEvent.class));
        }

        @Override
        public void processElement1(OrderEvent orderPay, CoProcessFunction<OrderEvent, ThirdPartyEvent, String>.Context ctx, Collector<String> out) throws Exception {
            if (thirdPartyEventState.value() == null){
                // 另一条流事件未到，保存当前事件到状态中，注册10秒后的定时器
                orderEventState.update(orderPay);
                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10000);
            } else {
                // 如果另一条流事件已经到来，成功匹配
                out.collect("订单" + orderPay.orderId + "对账成功！");
                thirdPartyEventState.clear();    // 清空状态
            }
        }

        @Override
        public void processElement2(ThirdPartyEvent thirdPartyPay, CoProcessFunction<OrderEvent, ThirdPartyEvent, String>.Context ctx, Collector<String> out) throws Exception {
            if (orderEventState.value() == null){
                // 另一条流事件未到，保存当前事件到状态中，注册10秒后的定时器
                thirdPartyEventState.update(thirdPartyPay);
                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10000);
            } else {
                // 如果另一条流事件已经到来，成功匹配
                out.collect("订单" + thirdPartyPay.orderId + "对账成功！");
                orderEventState.clear();    // 清空状态
            }
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<OrderEvent, ThirdPartyEvent, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定义输出标签，将报警信息输出到侧输出流
            OutputTag<String> outputTag = new OutputTag<String>("warning") {
            };

            // 定时器触发，判断哪个事件没来，输出报警信息
            if (orderEventState.value() != null)
                ctx.output(outputTag, "订单" + orderEventState.value().orderId + "第三方支付信息未到！");
            if (thirdPartyEventState.value() != null)
                ctx.output(outputTag, "订单" + thirdPartyEventState.value().orderId + "APP支付信息未到！");

            // 清空状态
            orderEventState.clear();
            thirdPartyEventState.clear();
        }
    }
}
