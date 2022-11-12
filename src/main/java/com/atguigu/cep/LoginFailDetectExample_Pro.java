package com.atguigu.cep;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

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

public class LoginFailDetectExample_Pro {
    public static void main(String[] args) throws Exception {
        // 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源
        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.fromElements(
                        new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                        new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                        new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                        new LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
                        new LoginEvent("user_2", "192.168.1.29", "success", 6000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(((element, recordTimestamp) -> element.ts))
                );

        // 1. 定义Pattern
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("fail")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return value.eventType.equals("fail");
                    }
                }).times(3).consecutive()
                .within(Time.seconds(10));

        // 2. 将模式应用到输入事件流上，检测匹配的复杂事件
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy(value -> value.userId), pattern);

        // 3. 处理检测到的复杂事件，输出报警信息
        patternStream.flatSelect(new PatternFlatSelectFunction<LoginEvent, String>() {
                    @Override
                    public void flatSelect(Map<String, List<LoginEvent>> pattern, Collector<String> out) throws Exception {
                        LoginEvent firstFail = pattern.get("fail").get(0);
                        LoginEvent secondFail = pattern.get("fail").get(1);
                        LoginEvent thirdFail = pattern.get("fail").get(2);
                        out.collect("用户" + firstFail.userId + "连续三次登录失败！登录时间：" +
                                firstFail.ts + ", " +
                                secondFail.ts + ", " +
                                thirdFail.ts);
                    }
                })
                .print();

        env.execute();
    }
}
