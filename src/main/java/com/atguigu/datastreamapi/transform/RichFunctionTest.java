package com.atguigu.datastreamapi.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class RichFunctionTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);

        stream.map(new RichMapFunction<Integer, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("任务开始！");
            }

            @Override
            public String map(Integer value) throws Exception {
                return value + " - " + getRuntimeContext().getIndexOfThisSubtask();
            }

            @Override
            public void close() throws Exception {
                System.out.println("任务结束");
            }
        }).print();

        env.execute();
    }
}
