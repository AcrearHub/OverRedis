package com.atguigu.stage;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 需求：去掉每个传感器相同的水位值
 */
public class Flink04_MapState {
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
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((event, ts) -> event.getTs())
                );

        test
                .keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            private MapState<Integer,Object> mapState;
                            @Override
                            public void open(Configuration parameters) {
                                MapStateDescriptor<Integer, Object> mapStateDescriptor = new MapStateDescriptor<>("mapState", Integer.class, Object.class);
                                mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                            }

                            @Override
                            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                                mapState.put(value.getVc(),null);
                                out.collect(ctx.getCurrentKey() + "上报的水位记录为：" + mapState.keys().toString());
                            }
                        }
                )
                .print();

        //启动程序执行
        env.execute();
    }
}
