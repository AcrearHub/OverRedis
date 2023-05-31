package com.atguigu.stage;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 需求：计算每种传感器水位和
 */
public class Flink05_ReducingState {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> test = env
                .socketTextStream("hadoop102", 8888)
                .map(s -> {
                    String[] str = s.split(",");
                    return new WaterSensor(str[0].trim(), Integer.valueOf(str[1].trim()), Long.valueOf(str[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((event, ts) -> event.getTs())
                );

        test
                .keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {
                            private ReducingState<Integer> reducingState;
                            @Override
                            public void open(Configuration parameters) {
                                ReducingStateDescriptor<Integer> reducingStateDescriptor = new ReducingStateDescriptor<>("reducingState", (ReduceFunction<Integer>) Integer::sum, Integer.class);
                                reducingState = getRuntimeContext().getReducingState(reducingStateDescriptor);
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                reducingState.add(value.getVc());
                                out.collect(ctx.getCurrentKey() + " 的水位和：" + reducingState.get());
                            }
                        }
                )
                .print();

        //启动程序执行
        env.execute();
    }
}
