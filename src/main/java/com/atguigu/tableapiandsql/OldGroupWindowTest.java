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

public class OldGroupWindowTest {
    public static void main(String[] args) throws Exception{
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

        // 统计每10秒中的用户访问数量
        Table userCountTable = tableEnv.sqlQuery("select user, count(*) as cnt, " +
                " TUMBLE_START(et, INTERVAL '10' SECOND), " +
                " TUMBLE_END(et, INTERVAL '10' SECOND)" +
                " from clicks " +
                " group by user, " +
                "          TUMBLE(et, INTERVAL '10' SECOND)");

        tableEnv.executeSql("create table user_cnt (" +
                " `user` string, " +
                " cnt bigint, " +
                " w_start timestamp(3)," +
                " w_end timestamp(3)" +
                ") with (" +
                " 'connector' = 'print'" +
                ")");

        userCountTable.executeInsert("user_cnt");
    }
}
