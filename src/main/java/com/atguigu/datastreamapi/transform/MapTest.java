package com.atguigu.datastreamapi.transform;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class MapTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickEventSource());

        // 提取Event中的user字段

        // 1. 传入一个MapFunction的实现类
        SingleOutputStreamOperator<String> result1 = stream.map(new MyMapper());

        // 2. 传入一个匿名类
        SingleOutputStreamOperator<String> result2 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event value) throws Exception {
                return value.url;
            }
        });

        // 3. lambda表达式
        SingleOutputStreamOperator<String> result3 = stream.map(value -> value.user);

        // 返回带泛型的类型数据
        SingleOutputStreamOperator<Tuple2<String, String>> result4 = stream.map(new MapFunction<Event, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Event value) throws Exception {
                return Tuple2.of(value.user, value.url);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, String>> result5 = stream
                .map(value -> Tuple2.of(value.user, value.url))
                .returns(Types.TUPLE(Types.STRING, Types.STRING));

        SingleOutputStreamOperator<Tuple2<String, String>> result6 = stream
                .map(value -> Tuple2.of(value.user, value.url), Types.TUPLE(Types.STRING, Types.STRING));

        result6.print("6");

        env.execute();
    }

    // 自定义一个MapFunction的实现类
    public static class MyMapper implements MapFunction<Event, String>{
        @Override
        public String map(Event value) throws Exception {
            return value.user;
        }
    }
}
