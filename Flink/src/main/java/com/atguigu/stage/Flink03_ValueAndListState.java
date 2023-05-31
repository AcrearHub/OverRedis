package com.atguigu.stage;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 值状态
 */
public class Flink03_ValueAndListState {
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
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    //定义值状态
                    private ValueState<WaterSensor> valueState;
                    private ListState<WaterSensor> listState;

                    /**
                     * 用open方法进行赋值
                     * @param parameters The configuration containing the parameters attached to the contract.
                     */
                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<WaterSensor> valueStateDescriptor = new ValueStateDescriptor<>("valueState", WaterSensor.class);
                        valueState = getRuntimeContext().getState(valueStateDescriptor);
                        ListStateDescriptor<WaterSensor> listStateDescriptor = new ListStateDescriptor<>("listState", WaterSensor.class);
                        listState = getRuntimeContext().getListState(listStateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //提取上次水位值
                        WaterSensor lastWs = valueState.value();
                        if (lastWs != null){
                            //判断两次水位值之差是否超过10
                            if (Math.abs(lastWs.getVc() - value.getVc()) > 10){
                                System.out.println("报警");
                            }
                        }
                        //更新状态
                        valueState.update(value);
                        out.collect(value.toString());

                        //对listvalue进行相关操作，这里省略
                    }
                })
                .print();

        //启动程序执行
        env.execute();
    }
}
