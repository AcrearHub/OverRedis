package com.atguigu.stage;

import com.atguigu.bean.AppEvent;
import com.atguigu.bean.ThirdPartEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 需求：实现一个实时对账的需求，也就是app的支付操作和第三方的支付操作的一个双流Join
 *      将对账结果全部输出，要求双方互等5s
 */
public class Test_Flink_BillCheckTestFinal {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        //从8888端口读取AppEvent：e.g：order-1,create,1000
        SingleOutputStreamOperator<AppEvent> app = env
                .socketTextStream("hadoop102", 8888)
                .map((MapFunction<String, AppEvent>) s -> {
                    String[] split = s.split(",");
                    return new AppEvent(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                })
                .filter((FilterFunction<AppEvent>) s -> "pay".equals(s.getEventType()))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<AppEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((event,ts) -> event.getTs())
                );

        //从9999端口读取ThirdPartEvent：e.g：order-1,pay,Alipay,3000
        SingleOutputStreamOperator<ThirdPartEvent> thirdPart = env
                .socketTextStream("hadoop102", 9999)
                .map((MapFunction<String, ThirdPartEvent>) s -> {
                    String[] split = s.split(",");
                    return new ThirdPartEvent(split[0].trim(), split[1].trim(), split[2].trim(), Long.valueOf(split[3].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<ThirdPartEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((event,ts) -> event.getTs())
                );

        //合流
        app
                .keyBy(AppEvent::getOrderId)
                .connect(thirdPart.keyBy(ThirdPartEvent::getOrderId))
                .process(
                        new KeyedCoProcessFunction<String, AppEvent, ThirdPartEvent, String>() {
                            private ValueState<AppEvent> appEventValueState;
                            private ValueState<ThirdPartEvent> thirdPartEventValueState;
                            @Override
                            public void open(Configuration parameters) {
                                ValueStateDescriptor<AppEvent> appDescriptor = new ValueStateDescriptor<>("appEnent", AppEvent.class);
                                ValueStateDescriptor<ThirdPartEvent> thirdPartDescriptor = new ValueStateDescriptor<>("thirdPart", ThirdPartEvent.class);
                                appEventValueState = getRuntimeContext().getState(appDescriptor);
                                thirdPartEventValueState = getRuntimeContext().getState(thirdPartDescriptor);
                            }

                            @Override
                            public void processElement1(AppEvent value, KeyedCoProcessFunction<String, AppEvent, ThirdPartEvent, String>.Context ctx, Collector<String> out) throws Exception {
                                //获取thirdpart的值
                                ThirdPartEvent thirdPartEvent = thirdPartEventValueState.value();
                                if (thirdPartEvent != null){
                                    out.collect(value.getOrderId() + "对账成功");
                                    //手动清楚状态
                                    thirdPartEventValueState.clear();
                                }else {
                                    appEventValueState.update(value);
                                    //注册5s后的定时器
                                    ctx.timerService().registerProcessingTimeTimer(value.getTs() + 5000L);
                                }
                            }

                            @Override
                            public void processElement2(ThirdPartEvent value, KeyedCoProcessFunction<String, AppEvent, ThirdPartEvent, String>.Context ctx, Collector<String> out) throws Exception {
                                AppEvent appEvent = appEventValueState.value();
                                if (appEvent != null){
                                    out.collect(value.getOrderId() + "对账成功");
                                    appEventValueState.clear();
                                }else {
                                    thirdPartEventValueState.update(value);
                                    ctx.timerService().registerEventTimeTimer(value.getTs() + 5000L);
                                }
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedCoProcessFunction<String, AppEvent, ThirdPartEvent, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                if (appEventValueState.value() != null){
                                    out.collect(appEventValueState.value().getOrderId() + "对账失败，第三方数据未到");
                                }else if (thirdPartEventValueState.value() != null){
                                    out.collect(thirdPartEventValueState.value().getOrderId() + "对账失败，app数据未到");
                                }
                                appEventValueState.clear();
                                thirdPartEventValueState.clear();
                            }
                        }
                )
                .print();

        //启动程序执行
        env.execute();
    }
}
