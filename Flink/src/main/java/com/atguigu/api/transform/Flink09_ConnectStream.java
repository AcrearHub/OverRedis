package com.atguigu.api.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Connect合流操作：可以是不同类型，只能两两合并
 *  两条流经过封装后，通过process将数据加工成相同类型再输出
 */
public class Flink09_ConnectStream {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> stream3 = env.fromElements("a", "b", "c");

        stream1
                .connect(stream3)
                .process(
                        new CoProcessFunction<Integer, String, String>() {

                            //处理第一条流的数据
                            @Override
                            public void processElement1(Integer value, CoProcessFunction<Integer, String, String>.Context ctx, Collector<String> out) {
                                out.collect(value.toString());
                            }

                            //处理第二条流的数据
                            @Override
                            public void processElement2(String value, CoProcessFunction<Integer, String, String>.Context ctx, Collector<String> out) {
                                out.collect(value);
                            }
                        }
                )
                .print("Connect");

        //启动程序执行
        env.execute();
    }
}
