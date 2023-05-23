package com.atguigu.api.transform;

import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * reduce测试：需求：统计当前访问量最大的用户
 */
public class Flink00_AggReduceTest {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        DataStreamSource<Event> test = env.addSource(new ClickSource());
        test
                .print("source");

        test
                .map((MapFunction<Event, Tuple2<String,Integer>>) event -> new Tuple2<>(event.getUser(), 1))
                .returns(new TypeHint<Tuple2<String, Integer>>() {})
                .keyBy(s -> s.f0)
                .reduce((ReduceFunction<Tuple2<String, Integer>>) (t1, t2) -> new Tuple2<>(t1.f0,t1.f1+t2.f1))
                .keyBy(s -> true)
                .reduce((ReduceFunction<Tuple2<String, Integer>>) (t1, t2) -> t1.f1>t2.f1?t1:t2)
                .print("reduce");

        //启动程序执行
        env.execute();
    }
}
