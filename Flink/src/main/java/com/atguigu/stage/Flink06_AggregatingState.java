package com.atguigu.stage;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 需求：计算每种传感器平均水位
 */
public class Flink06_AggregatingState {
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
                            private AggregatingState<Integer,Double> aggregatingState;
                            @Override
                            public void open(Configuration parameters) {
                                AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double> aggregatingStateDescriptor = new AggregatingStateDescriptor<>("aggregatingState", new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                    /**
                                     * 定义初始值
                                     */
                                    @Override
                                    public Tuple2<Integer, Integer> createAccumulator() {
                                        return Tuple2.of(0, 0);
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                        return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                                    }

                                    @Override
                                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                        return (double) (accumulator.f0 / accumulator.f1);
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                        return null;
                                    }
                                }, Types.TUPLE(Types.INT, Types.INT));
                                aggregatingState = getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                aggregatingState.add(value.getVc());
                                out.collect(ctx.getCurrentKey() + " 的平均水位：" + aggregatingState.get());
                            }
                        }
                )
                .print();

        //启动程序执行
        env.execute();
    }
}
