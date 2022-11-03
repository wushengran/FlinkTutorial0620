package com.atguigu.datastreamapi.sink;

import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class SinkToMySQLTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Alice", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./fav", 3000L),
                new Event("Cary", "./home", 4000L),
                new Event("Cary", "./prod?id=1", 5000L),
                new Event("Alice", "./home", 6000L),
                new Event("Alice", "./prod?id=100", 7000L),
                new Event("Mary", "./prod?id=1", 8000L),
                new Event("Mary", "./prod?id=100", 9000L)
        );

        stream.addSink( JdbcSink.<Event>sink(
                "insert into clicks (user, url) values (?, ?)",
                new JdbcStatementBuilder<Event>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
                        preparedStatement.setString(1, event.user);
                        preparedStatement.setString(2, event.url);
                    }
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        ) );

        env.execute();
    }
}
