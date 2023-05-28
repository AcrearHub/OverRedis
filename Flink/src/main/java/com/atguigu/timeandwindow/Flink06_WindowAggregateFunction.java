package com.atguigu.timeandwindow;

import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

/**
 * uv
 */

public class Flink06_WindowAggregateFunction {
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
                .keyBy(s -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new AggregateFunction<Event, HashSet, Long>() {
                            /**
                             * 创建累加器，窗口创建的时候调用一次
                             * @return
                             */
                            @Override
                            public HashSet createAccumulator() {
                                System.out.println("初始化累加器");
                                return new HashSet();
                            }

                            /**
                             * 累加过程，每条数据都要执行一次
                             * @param value The value to add
                             * @param accumulator The accumulator to add the value to
                             * @return
                             */
                            @Override
                            public HashSet add(Event value, HashSet accumulator) {
                                //提取user，添加到累加器中
                                accumulator.add(value.getUser());
                                return accumulator;
                            }

                            /**
                             * 获取累加器结果
                             * @param accumulator The accumulator of the aggregation
                             * @return
                             */
                            @Override
                            public Long getResult(HashSet accumulator) {
                                //返回set长度
                                return (long) accumulator.size();
                            }

                            /**
                             * 合并累加器，一般不用管
                             * @param a An accumulator to merge
                             * @param b Another accumulator to merge
                             * @return
                             */
                            @Override
                            public HashSet merge(HashSet a, HashSet b) {
                                return null;
                            }
                        }
                )
                .print();

        //启动程序执行
        env.execute();
    }
}
