package com.atguigu.tableapiandsql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class OverWindowTest {
    public static void main(String[] args) {
        // 统计每个用户之前3次访问的平均时间戳
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());

        tableEnv.executeSql("create table clicks (" +
                "  `user` string, " +
                "  url string, " +
                "  ts bigint, " +
                "  et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +    // 事件时间属性，TIMESTAMP类型
//                "  et AS TO_TIMESTAMP_LTZ(ts, 3), " +     // 事件时间属性，TIMESTAMP_LTZ
                "  WATERMARK FOR et AS et - INTERVAL '2' SECOND " +     // 水位线生成策略
                ") with (" +
                "  'connector' = 'filesystem', " +
                "  'path' = 'input/clicks.txt', " +
                "  'format' = 'csv'" +
                ") ");

        Table result = tableEnv.sqlQuery("select user, ts, avg(ts) OVER (" +
                "  PARTITION BY user " +
                "  ORDER BY et " +
                "  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW" +
                ") as avg_ts " +
                "from clicks");

        tableEnv.executeSql("create table output (" +
                " `user` string, " +
                "  ts bigint, " +
                "  avg_ts bigint" +
                ") with (" +
                "  'connector' = 'print'" +
                ")");
        result.executeInsert("output");
    }
}
