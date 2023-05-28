package com.atguigu.timeandwindow;

import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 全窗口函数：窗口中的数据全部收集齐再计算；可以获取到窗口信息
 * 需求：统计10s内用户点击信息，和窗口信息
 */
public class Flink08_WindowProcessWindowFunction {
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
                .keyBy(Event::getUser)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(
                        new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                            /**
                             *
                             * @param s 当前窗口的key
                             * @param context 上下文，可获取当前窗口信息
                             * @param elements 当前窗口中所有数据
                             * @param out 输出收集器
                             */
                            @Override
                            public void process(String s, ProcessWindowFunction<Event, String, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) {
                                //统计当前key的点击次数
                                long count = 0L;
                                for (Event ignored : elements) {
                                    count++;
                                }

                                //获取窗口信息
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                out.collect("["+start+'~'+end+")窗口中，用户"+s+"的点击次数为"+count);
                            }
                        }
                )
                .print();


        //启动程序执行
        env.execute();
    }
}
