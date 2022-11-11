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

public class WindowTVFTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                );

        Table eventTable = tableEnv.fromDataStream(stream, $("user"), $("url"),
                $("timestamp").as("ts"), $("et").rowtime(), $("pt").proctime());

        tableEnv.createTemporaryView("clicks", eventTable);

        // 滚动窗口
        Table tumbleCountTable = tableEnv.sqlQuery("select user, count(*) as cnt, " +
                " window_start, " +
                " window_end" +
                " from TABLE(" +
                "  TUMBLE(TABLE clicks, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                ") " +
                " group by user, " +
                "          window_start, " +
                "          window_end");

//        tableEnv.toDataStream(tumbleCountTable).print("tumble");

        // 滑动窗口
        Table hopCountTable = tableEnv.sqlQuery("select user, count(*) as cnt ," +
                " window_start, " +
                " window_end" +
                " from TABLE (" +
                "  HOP(TABLE clicks, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND)" +
                ") group by user, window_start, window_end");
        tableEnv.toDataStream(hopCountTable).print("hop");

        // 累积窗口
        Table cumulateCountTable = tableEnv.sqlQuery("select user, count(*) as cnt ," +
                " window_start, " +
                " window_end" +
                " from TABLE (" +
                "  CUMULATE(TABLE clicks, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND)" +
                ") group by user, window_start, window_end");
        tableEnv.toDataStream(cumulateCountTable).print("cumulate");

        env.execute();
    }
}
