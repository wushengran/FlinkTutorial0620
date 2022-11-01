package com.atguigu.wc;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class StreamWordCount {
    public static void main(String[] args) throws Exception{
        // 1. 获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        env.setParallelism(1);

        // 2. 读取数据
//        DataStreamSource<String> dataStreamSource = env.readTextFile("input/words.txt");

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStreamSource<String> dataStreamSource = env.socketTextStream(host, port);

        // 3. 转换处理
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = dataStreamSource.flatMap(
                        (String line, Collector<Tuple2<String, Long>> out) -> {
                            String[] words = line.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1L));
                            }
                        }
                )
//                .returns(new TypeHint<Tuple2<String, Long>>() {})
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(value -> value.f0)
                .sum(1);

        sum.print();

        // 执行
        env.execute();
    }
}
