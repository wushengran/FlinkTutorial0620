package com.atguigu.tableapiandsql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class CommonAPITest {
    public static void main(String[] args) {
        // 1.表执行环境
        // 1.1 基于流式执行环境创建
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // 1.2 直接创建
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(environmentSettings);

        // 2. 创建表
        // 2.1 创建连接器表
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


        // 2.2 创建虚拟表，需要基于一个已经存在的Table对象

        // 3. 基于表进行查询转换
        // 3.1 基于Table API
        // 要调用Table API，必须先得到Table对象
        Table clicks = tableEnv.from("clicks");
        Table result = clicks.where($("url").isEqual(" ./home"))
                .select($("url"), $("user"), $("ts"));

        // 3.2 SQL
        // 要执行SQL，必须基于已经在表环境中注册的表
        tableEnv.createTemporaryView("home_clicks", result);

        // 3.2.1 基本转换
        Table result2 = tableEnv.sqlQuery("select user, url ,ts from home_clicks where user = 'Bob'");

        // 3.2.2 分组聚合转换
        Table userCountTable = tableEnv.sqlQuery("select user, count(*) as cnt from clicks group by user");

        // 4. 表的输出
        // 创建一张连接器表用于输出
        String createOutTableDDL = "CREATE TABLE output (" +
                " `user` VARCHAR(20), " +
                " url STRING, " +
                " ts BIGINT" +
                ") WITH (" +
                " 'connector' = 'print' " +
                ")";
        tableEnv.executeSql(createOutTableDDL);

        result2.executeInsert("output");

        // 创建一张连接器表用于输出
        String createCountTableDDL = "CREATE TABLE user_count (" +
                " `user` VARCHAR(20), " +
                " cnt BIGINT" +
                ") WITH (" +
                " 'connector' = 'print' " +
                ")";
        tableEnv.executeSql(createCountTableDDL);

        userCountTable.executeInsert("user_count");
    }
}
