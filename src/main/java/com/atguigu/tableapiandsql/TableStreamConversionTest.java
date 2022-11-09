package com.atguigu.tableapiandsql;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class TableStreamConversionTest {
    public static void main(String[] args) throws Exception{
        // 只有流式的表环境，才可以执行流标转换操作
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 测试表转换成流
        // 创建输入表
        String createTableDDL = "CREATE TABLE clicks (" +
                " `user` VARCHAR(20), " +
                " url STRING, " +
                " ts BIGINT" +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.txt', " +
                " 'format' = 'csv'" +
                ")";
        tableEnv.executeSql(createTableDDL);

        // 执行转换计算
        Table simpletransTable = tableEnv.sqlQuery("select user, url ,ts from clicks where user = 'Bob'");

        Table userCountTable = tableEnv.sqlQuery("select user, count(*) as cnt from clicks group by user");

//        tableEnv.toDataStream(simpletransTable).print("simple");
//        tableEnv.toChangelogStream(userCountTable).print("user_count");

        // 2. 流转换成表
        DataStreamSource<Event> stream = env.addSource(new ClickEventSource());
        Table table = tableEnv.fromDataStream(stream, $("user").as("uname"), $("timestamp").as("ts"),$("url"));

//        tableEnv.fromChangelogStream()

        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5, 6);
        Table intTable = tableEnv.fromDataStream(intStream, $("myint"));

        DataStreamSource<Tuple2<String, Integer>> tuple2Stream = env.fromElements(Tuple2.of("a", 1), Tuple2.of("b", 2), Tuple2.of("c", 3), Tuple2.of("ef", 5));
        Table tuple2Table = tableEnv.fromDataStream(tuple2Stream, $("mystring"), $("myint"));

        table.printSchema();
        tableEnv.toDataStream(table).print();

        env.execute();
    }
}
