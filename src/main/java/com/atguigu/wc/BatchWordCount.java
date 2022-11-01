package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class BatchWordCount {
    public static void main(String[] args)  throws Exception{
        // 1. 获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 从文件读取数据
        DataSource<String> dataSource = env.readTextFile("input/words.txt");

        // 3. 对数据进行转换处理，将每行数据打散包装成(word, 1)二元组
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuples = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = line.split(" ");
                // 遍历每个单词，包装二元组输出
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        // 4. 按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> grouping = wordAndOneTuples.groupBy(0);

        // 5. 按照数量进行累加
        AggregateOperator<Tuple2<String, Long>> sum = grouping.sum(1);

        sum.print();
    }
}
