package com.atguigu.timeandwindow;

import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * pv
 */
public class Flink05_WindowReduceFunction {
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
                .map(s -> Tuple2.of(s.getUser(),1))
                .returns(new TypeHint<Tuple2<String, Integer>>() {})
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce((value1, value2) -> Tuple2.of(value1.f0,value1.f1+value2.f1))
                .print();

        //启动程序执行
        env.execute();
    }
}
