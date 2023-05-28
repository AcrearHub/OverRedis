package com.atguigu.timeandwindow;

import com.atguigu.bean.Event;
import com.atguigu.bean.URLViewCount;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 增、全量函数的配合使用：AggregateFunction + ProcessWindowFunction
 * 先使用增量函数输出结果，再用全量函数获取窗口相关信息输出结果
 * 需求：统计10s内url的点击次数，加上窗口信息
 */

public class Flink09_WindowAggAndFullWindowFunction {
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
                .keyBy(Event::getUrl)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        //通过acc统计url的点击次数
                        new AggregateFunction<Event, Long, URLViewCount>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(Event value, Long accumulator) {
                                return accumulator + 1;
                            }

                            @Override
                            public URLViewCount getResult(Long accumulator) {
                                return new URLViewCount(null, null, null, accumulator);
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return null;
                            }
                        }
                        //加上窗口信息
                        , new ProcessWindowFunction<URLViewCount, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<URLViewCount, String, String, TimeWindow>.Context context, Iterable<URLViewCount> elements, Collector<String> out) {
                                //获取窗口数据
                                URLViewCount next = elements.iterator().next();
                                //获取窗口信息
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                next.setUrl(s);
                                next.setStart(start);
                                next.setEnd(end);
                                out.collect(next.toString());
                            }
                        }
                )
                .print();

        //启动程序执行
        env.execute();
    }
}
