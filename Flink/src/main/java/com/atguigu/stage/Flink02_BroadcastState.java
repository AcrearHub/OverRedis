package com.atguigu.stage;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 广播状态
 */
public class Flink02_BroadcastState {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        //数据流
        DataStreamSource<String> data = env.socketTextStream("hadoop102", 8888);

        //配置流
        DataStreamSource<String> broad = env.socketTextStream("hadoop102", 9999);

        //新建状态描述器
        MapStateDescriptor<String, String> stringMapStateDescriptor = new MapStateDescriptor<>("广播流", String.class, String.class);
        //将配置流处理成广播流
        BroadcastStream<String> broadcastStream = broad.broadcast(stringMapStateDescriptor);

        //链接两条流
        data
                .connect(broadcastStream)
                .process(new BroadcastProcessFunction<String, String, String>() {
                    /**
                     * 处理数据流的数据
                     */
                    @Override
                    public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        //从广播状态中读配置
                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(stringMapStateDescriptor);
                        //获取状态
                        String conf = broadcastState.get("conf");
                        if ("1".equals(conf))
                            System.out.println("加载1号配置");
                        else
                            System.out.println("加载默认配置");
                    }

                    /**
                     * 处理广播流的数据
                     */
                    @Override
                    public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                        //获取广播状态
                        BroadcastState<String, String> broadcastState = ctx.getBroadcastState(stringMapStateDescriptor);
                        //将广播流数据存入状态中
                        broadcastState.put("conf",value);
                    }
                });
        //启动程序执行
        env.execute();
    }
}
