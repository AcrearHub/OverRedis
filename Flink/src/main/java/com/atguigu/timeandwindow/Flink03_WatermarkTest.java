package com.atguigu.timeandwindow;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 水位线测试
 */
public class Flink03_WatermarkTest {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",5678);
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
//      env.setParallelism(1);

        SingleOutputStreamOperator<Event> test = env
                .socketTextStream("hadoop102", 8888)
                .map((MapFunction<String, Event>) value -> {
                    String[] s = value.split(",");
                    return new Event(s[0].trim(), s[1].trim(), Long.valueOf(s[3].trim()));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.getTs())
                );

        test
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) {
                        long currentWatermark = ctx.timerService().currentWatermark();
                        long currentProcessingTime = ctx.timerService().currentProcessingTime();
                        out.collect(JSON.toJSONString(value)+"--"+currentWatermark+"--"+currentProcessingTime);
                    }
                })
                .setParallelism(4)
                .print();

        //启动程序执行
        env.execute();
    }
}
