package com.atguigu.tableapiandsql;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
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

public class TimeAttributeTest {
    public static void main(String[] args) throws Exception{
        // 1. 不依赖流式执行环境，直接在创建表的DDL中定义
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        tableEnv.executeSql("create table clicks (" +
                "  `user` string, " +
                "  url string, " +
                "  ts bigint, " +
//                "  et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +    // 事件时间属性，TIMESTAMP类型
                "  et AS TO_TIMESTAMP_LTZ(ts, 3), " +     // 事件时间属性，TIMESTAMP_LTZ
                "  WATERMARK FOR et AS et - INTERVAL '2' SECOND, " +     // 水位线生成策略
                "  pt AS PROCTIME()" +    // 处理时间属性
                ") with (" +
                "  'connector' = 'filesystem', " +
                "  'path' = 'input/clicks.txt', " +
                "  'format' = 'csv'" +
                ") ");

        tableEnv.from("clicks").printSchema();

        // 2. 在流转换成表的时候定义时间属性
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                );

        Table eventTable = tableEnvironment.fromDataStream(stream, $("user"), $("url"),
                $("timestamp").rowtime().as("ts"), $("pt").proctime());

        eventTable.printSchema();

        env.execute();
    }
}
