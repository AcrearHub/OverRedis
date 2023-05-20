package com.atguigu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 无界流处理
 */
public class WordCountStreamUnBounded {
    public static void main(String[] args) throws Exception {
        //1、创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        //2、获取外部传入参数，从socket端口读数据
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

            //不指定时默认传hadoop102
        String hostname;
        hostname = parameterTool.get("hostname");
        if (hostname==null || hostname.equals("")){
            hostname = "hadoop102";
        }
            //不指定时默认传8888
        int port;
        try {
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            port = 8888;
        }

        DataStreamSource<String> stringDataStreamSource = env.socketTextStream(hostname,port);

        //3、调用Flink API进行转换处理
        stringDataStreamSource
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (words, collector) -> {
                    String[] word = words.split(" ");
                    for (String s : word) {
                        collector.collect(Tuple2.of(s,1));
                    }
                })
                .returns(new TypeHint<Tuple2<String, Integer>>() {})
                //做并行处理时有分组的作用
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) stringIntegerTuple2 -> stringIntegerTuple2.f0)
        //4、汇总、输出结果
                .sum(1)
                .print();
        //5、启动程序执行
        env.execute();
    }
}
