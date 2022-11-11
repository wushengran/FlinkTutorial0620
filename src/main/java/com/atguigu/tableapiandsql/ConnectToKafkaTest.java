package com.atguigu.tableapiandsql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class ConnectToKafkaTest {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        tableEnv.executeSql("create table clicks (" +
                "  `user` string, " +
                "  url string, " +
                "  ts bigint " +
                ") with (" +
                "  'connector' = 'kafka', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'topic' = 'clicks', " +
                "  'format' = 'csv'" +
                ") ");

        tableEnv.executeSql("create table bob_clicks (" +
                "  url string, " +
                "  `user` string, " +
                "  ts bigint " +
                ") with (" +
                "  'connector' = 'kafka', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'topic' = 'bob_clicks', " +
                "  'format' = 'csv'" +
                ") ");

//        tableEnv.executeSql("insert into bob_clicks select url, user, ts from clicks where user='Bob'")

        tableEnv.executeSql("create table user_count (" +
                "  uname string, " +
                "  cnt bigint," +
                "  PRIMARY KEY (uname) NOT ENFORCED" +
                ") with (" +
                "  'connector' = 'upsert-kafka', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'topic' = 'user_count', " +
                "  'key.format' = 'csv', " +
                "  'value.format' = 'csv' " +
                ") ");

        tableEnv.executeSql("insert into user_count select user, count(*) from clicks group by user");
    }
}
