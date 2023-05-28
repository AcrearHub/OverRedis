package com.atguigu.timeandwindow;

import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 1、窗口分配器：执行什么类型窗口，窗口大小、滑动步长等
 * 2、窗口算子：对窗口中的数据进行处理
 */
public class Flink04_WindowIntroduce {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> test = env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((a, b) -> a.getTs())
                );

        //窗口划分：统计每10s用户的点击次数
        test
                .map(s -> Tuple1.of(1))
                .returns(new TypeHint<Tuple1<Integer>>() {})
                .windowAll(
                        //窗口分配器
                        TumblingProcessingTimeWindows.of(Time.days(1),Time.hours(-8))  //处理时间滚动窗口，带时区处理
                )
                .sum(0)
                .print();

        //启动程序执行
        env.execute();
    }
}
