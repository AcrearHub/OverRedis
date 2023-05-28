package com.atguigu.timeandwindow;

import com.atguigu.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 对迟到数据的处理
 */
public class Flink10_LateData {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> source = env
                .socketTextStream("hadoop102",8888)
                .map((MapFunction<String, Event>) value -> {
                    String[] s = value.split(",");
                    return new Event(s[0].trim(), s[1].trim(), Long.valueOf(s[3].trim()));
                })
                .assignTimestampsAndWatermarks(
                        //乱序流
                        WatermarkStrategy
                                .<Event>forBoundedOutOfOrderness(Duration.ZERO)    //设置乱序流延迟
                                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.getTs())    //从数据源获取时间
                );
        source.print("source");

        OutputTag<Tuple2<String, Integer>> outputTag = new OutputTag<Tuple2<String, Integer>>("late") {};

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = source
                .map(s -> Tuple2.of(s.getUser(), 1))
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                })
                .keyBy(s -> s.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(outputTag)
                .sum(1);

        sum.getSideOutput(outputTag).print();
        sum.print();

        //启动程序执行
        env.execute();
    }
}
