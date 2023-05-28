package com.atguigu.timeandwindow;

import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * 需求：页面浏览量pv/独立访客数uv：求人均访问量
 * 思路：使用事件时间滚动窗口30s，使用aggregate完成统计：输入类型event，输出类型double，累加器Tuple2<HashSet（维护uv）,Long（维护pv）>
 */
public class Flink07_WindowTest_PVUV {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> source = env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        //乱序流
                        WatermarkStrategy
                                .<Event>forBoundedOutOfOrderness(Duration.ZERO)    //设置乱序流延迟
                                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.getTs())    //从数据源获取时间
                );
        source.print("source");

        source
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new AggregateFunction<Event, Tuple2<HashSet<String>,Long>, Double>() {
                            @Override
                            public Tuple2<HashSet<String>, Long> createAccumulator() {
                                return Tuple2.of(new HashSet<>(),0L);
                            }

                            @Override
                            public Tuple2<HashSet<String>, Long> add(Event value, Tuple2<HashSet<String>, Long> accumulator) {
                                //将pv累加到f1
                                accumulator.f1 = accumulator.f1+1;
                                //将uv累加到f0
                                accumulator.f0.add(value.getUser());
                                return accumulator;
                            }

                            @Override
                            public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
                                return (double) accumulator.f1/accumulator.f0.size();
                            }

                            @Override
                            public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a, Tuple2<HashSet<String>, Long> b) {
                                return null;
                            }
                        }
                )
                .print();

        //启动程序执行
        env.execute();
    }
}
