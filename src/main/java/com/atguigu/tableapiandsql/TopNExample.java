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

public class TopNExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                );
        stream.print("input");

        // 将数据流转换成表
        Table clicks = tableEnv.fromDataStream(stream, $("user"), $("url"), $("timestamp").as("ts"),
                $("et").rowtime());
        tableEnv.createTemporaryView("clicks", clicks);

        // 1. 计算每个url在滚动窗口中的访问量
        Table urlCountTable = tableEnv.sqlQuery("select url, count(*) as cnt, window_start, window_end " +
                "from TABLE (" +
                "  TUMBLE(TABLE clicks, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                ")" +
                "group by url, window_start, window_end ");
        tableEnv.toDataStream(urlCountTable).print("url-count");

        // 2. 计算每个窗口内，按照url的count值排序之后的行号进行提取，选取top n
        tableEnv.createTemporaryView("url_count", urlCountTable);
        Table topNTable = tableEnv.sqlQuery("select * from (" +
                "  select window_start, window_end, url, cnt, ROW_NUMBER() OVER (" +
                "    PARTITION BY window_start, window_end " +
                "    ORDER BY cnt DESC " +
                "  ) as row_num " +
                "  from url_count " +
                ")" +
                "where row_num <= 3");

        tableEnv.toChangelogStream(topNTable).print();

        env.execute();
    }
}
