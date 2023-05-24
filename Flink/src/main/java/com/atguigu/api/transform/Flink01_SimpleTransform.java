package com.atguigu.api.transform;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 基本转换算子
 * map：映射
 * filter：过滤
 * flatMap：扁平映射
 */
public class Flink01_SimpleTransform {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        DataStreamSource<Event> test = env.addSource(new ClickSource());
        test
                .print("source");

        //map测试：将得到的数据转为JSON
        test
                .map(JSON::toJSONString)
                .print("map");

        //filter测试：将得到的数据中user为Tom的过滤出来
        test
                .filter((FilterFunction<Event>) event -> "Tom".equals(event.getUser()))
                .print("filter");

        //flatMap测试：将得到的数据中user为Tom的过滤出来，并将该数据拆分成字段输出
        test
                .flatMap((FlatMapFunction<Event, String>) (event, collector) -> {
                    if ("Tom".equals(event.getUser())){
                        collector.collect("user："+event.getUser());
                        collector.collect("url："+event.getUrl());
                        collector.collect("ts："+event.getTs());
                    }
                })
                .returns(String.class)
                .print("flatMap");

        //启动程序执行
        env.execute();
    }
}
