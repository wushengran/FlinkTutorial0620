package com.atguigu.datastreamapi.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class SourceTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 通用读取数据源的方法
//        env.addSource()

        // 1. 读取集合数据
        ArrayList<Integer> integers = new ArrayList<>();
        integers.add(1);
        integers.add(23);
        integers.add(456);
        DataStream<Integer> stream1 = env.fromCollection(integers);

        stream1.print("stream1");

        // 2. 读取元素数据
        DataStreamSource<String> stream2 = env.fromElements("abc", "de", "fghi");
        stream2.print("stream2");

        // 3. 读取文本文件
        DataStreamSource<String> stream3 = env.readTextFile("input/words.txt");

        // 4. 读取socket文本流
        DataStreamSource<String> stream4 = env.socketTextStream("hadoop102", 7777);

        env.execute();
    }
}
