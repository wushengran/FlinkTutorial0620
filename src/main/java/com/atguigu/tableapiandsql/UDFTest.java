package com.atguigu.tableapiandsql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class UDFTest {
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

        // 注册自定义标量函数
        tableEnv.createTemporarySystemFunction("myhash", MyHash.class);

        tableEnv.executeSql("create table output (" +
                " `user` string, " +
                " user_hash int, " +
                " url string, " +
                " url_hash int" +
                ") with (" +
                " 'connector' = 'print'" +
                ")");

        tableEnv.executeSql("insert into output select user, myhash(user), url, myhash(url) from clicks");
    }

    // 自定义标量函数，求字符串的hash值
    public static class MyHash extends ScalarFunction{
        public int eval(String str){
            return str.hashCode();
        }
    }
}
