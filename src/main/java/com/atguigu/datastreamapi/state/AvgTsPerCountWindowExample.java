package com.atguigu.datastreamapi.state;

import com.atguigu.datastreamapi.source.ClickEventSource;
import com.atguigu.datastreamapi.source.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class AvgTsPerCountWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(((element, recordTimestamp) -> element.timestamp))
                );

        stream.print("input");

        // 模拟一个滚动计数窗口，统计每个用户连续5次访问的平均时间戳
        stream.keyBy(value -> value.user)
                .flatMap( new AvgTsPerCountWindow(5) )
                .print();

        env.execute();
    }

    // 实现自定义的富函数类
    public static class AvgTsPerCountWindow extends RichFlatMapFunction<Event, String>{
        private int size;
        // 使用聚合状态保存增量聚合的平均时间戳
        AggregatingState<Event, Tuple2<Integer, Long>> avgTsAggState;

        public AvgTsPerCountWindow(int size) {
            this.size = size;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            avgTsAggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Integer>, Tuple2<Integer, Long>>(
                    "avg-ts",
                    new AggregateFunction<Event, Tuple2<Long, Integer>, Tuple2<Integer, Long>>() {
                        @Override
                        public Tuple2<Long, Integer> createAccumulator() {
                            return Tuple2.of(0L, 0);
                        }

                        @Override
                        public Tuple2<Long, Integer> add(Event value, Tuple2<Long, Integer> accumulator) {
                            return Tuple2.of( accumulator.f0 + value.timestamp, accumulator.f1 + 1 );
                        }

                        @Override
                        public Tuple2<Integer, Long> getResult(Tuple2<Long, Integer> accumulator) {
                            return Tuple2.of(accumulator.f1, accumulator.f0 / accumulator.f1);
                        }

                        @Override
                        public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
                            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                        }
                    },
                    Types.TUPLE(Types.LONG, Types.INT)
            ));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            // 每来一个数据，就添加到聚合状态
            avgTsAggState.add(value);

            // 判断统计的个数是否达到了size
            if (avgTsAggState.get().f0 >= size){
                out.collect("用户" + value.user + "连续" + size + "次访问的平均时间戳为：" + avgTsAggState.get().f1);
                // 清空状态
                avgTsAggState.clear();
            }
        }
    }
}
