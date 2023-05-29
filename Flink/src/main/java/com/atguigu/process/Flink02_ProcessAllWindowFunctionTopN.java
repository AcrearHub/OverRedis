package com.atguigu.process;

import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * 基于ProcessAllWindowFunction实现：求url点击次数TopN
 */
public class Flink02_ProcessAllWindowFunctionTopN {
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
                //只保留url即可
                .map(Event::getUrl)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) {
                        //迭代url数据，统计点击次数
                        HashMap<String, Long> urlCount = new HashMap<>();
                        for (String element : elements) {
                            //从map中尝试获取key
                            Long value = urlCount.getOrDefault(element, 0L);
                            //将新的统计结果放到map中
                            urlCount.put(element,value + 1);
                        }

                        //转换成list进行排序
                        ArrayList<Tuple2<String, Long>> tuple2s = new ArrayList<>();
                        for (String s : urlCount.keySet()) {
                            Long value = urlCount.get(s);
                            tuple2s.add(Tuple2.of(s,value));
                        }

                        tuple2s.sort((o1, o2) -> Long.compare(o2.f1,o1.f1));

                        StringBuilder stringBuilder = new StringBuilder("==窗口==\n");
                        for (int i = 0; i < Math.min(3, tuple2s.size()); i++) {
                            Tuple2<String, Long> aa = tuple2s.get(i);
                            stringBuilder.append("No.").append(i + 1).append(" URL：").append(aa.f0).append("--").append(aa.f1).append("\n");
                        }
                        out.collect(String.valueOf(stringBuilder));
                    }
                })
                .print();

        //启动程序执行
        env.execute();
    }
}
