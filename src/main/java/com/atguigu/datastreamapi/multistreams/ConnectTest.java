package com.atguigu.datastreamapi.multistreams;

import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import scala.Int;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class ConnectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取两条流数据
        DataStreamSource<Integer> numStream = env.fromElements(1, 2, 3, 4);
        DataStreamSource<String> stringStream = env.fromElements("a", "b", "c");

        // 连接不同类型的两条流
        numStream.connect(stringStream)
                .flatMap(new CoFlatMapFunction<Integer, String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap1(Integer value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(Tuple2.of("number", value));
                    }

                    @Override
                    public void flatMap2(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(Tuple2.of(value, 0));
                    }
                })
                .print();

        env.execute();
    }
}
