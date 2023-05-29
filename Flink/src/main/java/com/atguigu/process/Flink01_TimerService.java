package com.atguigu.process;

import com.atguigu.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 定时服务
 */
public class Flink01_TimerService {
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

        //调用process算子
        source
                .keyBy(Event::getUser)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        TimerService timerService = ctx.timerService();
                        long processingTime = timerService.currentProcessingTime();
                        long watermark = timerService.currentWatermark();
                        //处理时间定时器为其5s后
                        timerService.registerProcessingTimeTimer(processingTime + 5000L);
                        out.collect(value.toString());
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("触发了定时器");
                    }
                })
                .print();

        //启动程序执行
        env.execute();
    }
}
