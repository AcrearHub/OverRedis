package com.atguigu.timeandwindow;

import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 内置生成策略
 * 1、不生成水位线
 * 2、有序流水位线
 * 3、乱序流水位线
 * 在这里，有序、乱序的区别只是需不需要设置乱序流延迟
 */
public class Flink02_FlinkGeneratorWatermark {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        //有序流
//                      WatermarkStrategy
//                              .<Event>forMonotonousTimestamps()
//                              .withTimestampAssigner(
//                                      (SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.getTs()
//                              )
                        //乱序流
                        WatermarkStrategy
                                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2L))    //设置乱序流延迟
                                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.getTs())    //从数据源获取时间
                )
                .print();

        //启动程序执行
        env.execute();
    }
}
