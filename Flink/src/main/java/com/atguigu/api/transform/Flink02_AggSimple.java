package com.atguigu.api.transform;

import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 简单聚合：sum、min、max、minBy、maxBy
 * 聚合前必须有keyBy操作
 */
public class Flink02_AggSimple {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        DataStreamSource<Event> test = env.addSource(new ClickSource());
        test
                .print("source");

        //sum测试：求每个人的点击次数
        test
                .map((MapFunction<Event, Tuple2<String,Integer>>) event -> new Tuple2<>(event.getUser(), 1))
                .returns(new TypeHint<Tuple2<String, Integer>>() {})
                .keyBy(s -> s.f0)
                .sum(1)
                .print("sum");

        //min、max测试：求每个人最新点击数据
        test
                .keyBy(Event::getUser)
                .max("ts")
                .print("max");

        //minBy、maxBy测试：求每个人最新点击数据
        test
                .keyBy(Event::getUser)
                .maxBy("ts")
                .print("maxBy");

        //启动程序执行
        env.execute();
    }
}
