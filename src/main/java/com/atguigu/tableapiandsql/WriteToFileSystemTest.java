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

public class WriteToFileSystemTest {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        tableEnv.executeSql("create table clicks (" +
                "  `user` string, " +
                "  url string, " +
                "  ts bigint, " +
//                "  et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +    // 事件时间属性，TIMESTAMP类型
                "  et AS TO_TIMESTAMP_LTZ(ts, 3), " +     // 事件时间属性，TIMESTAMP_LTZ
                "  WATERMARK FOR et AS et - INTERVAL '2' SECOND " +     // 水位线生成策略
                ") with (" +
                "  'connector' = 'filesystem', " +
                "  'path' = 'input/clicks.txt', " +
                "  'format' = 'csv'" +
                ") ");

        tableEnv.executeSql("create table bob_clicks (" +
                "  url string, " +
                "  `user` string, " +
                "  ts bigint " +
                ") with (" +
                "  'connector' = 'filesystem', " +
                "  'path' = 'output', " +
                "  'format' = 'csv'" +
                ") ");

        tableEnv.executeSql("insert into bob_clicks select url, user, ts from clicks where user='Bob'");
    }
}
