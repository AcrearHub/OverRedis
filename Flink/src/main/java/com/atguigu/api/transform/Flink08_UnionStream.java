package com.atguigu.api.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Union合流操作：必须是同类型，可以多条流合并
 */
public class Flink08_UnionStream {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> stream2 = env.fromElements(6, 7, 8, 9, 10);

        stream1
                .union(stream2)
                .print("union");

        //启动程序执行
        env.execute();
    }
}
