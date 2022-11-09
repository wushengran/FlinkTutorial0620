package com.atguigu.tableapiandsql;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class TableAPIAndSQLTest {
    public static void main(String[] args) throws Exception{
        // 先创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                );

        // 创建一个流式表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将流转换成表
        Table table = tableEnv.fromDataStream(stream);

        // 调用 table api 进行转换计算，提取所有Alice用户的访问数据(user, url)
        Table result = table.select($("user"), $("url"))
                .where($("user").isEqual("Alice"));

        // 直接写 SQL 实现转换计算
        Table result2 = tableEnv.sqlQuery("select user, url from " + table + " where user = 'Bob'");

//        result.printSchema();

        // 将表转换流打印输出
        tableEnv.toDataStream(result).print("alice");
        tableEnv.toDataStream(result2).print("bob");

        env.execute();
    }
}
