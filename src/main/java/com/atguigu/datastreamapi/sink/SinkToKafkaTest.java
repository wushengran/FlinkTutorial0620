package com.atguigu.datastreamapi.sink;

import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<Event> stream = env.fromElements(
//                new Event("Alice", "./home", 1000L),
//                new Event("Bob", "./cart", 2000L),
//                new Event("Alice", "./fav", 3000L),
//                new Event("Cary", "./home", 4000L),
//                new Event("Cary", "./prod?id=1", 5000L),
//                new Event("Alice", "./home", 6000L),
//                new Event("Alice", "./prod?id=100", 7000L),
//                new Event("Mary", "./prod?id=1", 8000L),
//                new Event("Mary", "./prod?id=100", 9000L)
//        );
//
//        stream.map( Event::toString )
//                .addSink(
//                        new FlinkKafkaProducer<String>("hadoop102:9092",
//                                "events",
//                                new SimpleStringSchema())
//                );

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        stream.map(value -> {
                    String[] fields = value.split(",");
                    return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                })
                .map(Event::toString)
                .addSink(new FlinkKafkaProducer<String>("events", new SimpleStringSchema(), properties));

        env.execute();
    }
}
