package com.atguigu.timeandwindow;

import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * orderInfo和orderDetail的连结
 */
public class Flink12_WindowIntervalJoin {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        SingleOutputStreamOperator<OrderInfo> source1 = env
                .socketTextStream("hadoop102",8888)
                .map((MapFunction<String, OrderInfo>) value -> {
                    String[] s = value.split(",");
                    return new OrderInfo(s[0].trim(), Long.valueOf(s[1].trim()));
                })
                .assignTimestampsAndWatermarks(
                        //乱序流
                        WatermarkStrategy
                                .<OrderInfo>forBoundedOutOfOrderness(Duration.ZERO)    //设置乱序流延迟
                                .withTimestampAssigner((SerializableTimestampAssigner<OrderInfo>) (element, recordTimestamp) -> element.getTs())    //从数据源获取时间
                );

        SingleOutputStreamOperator<OrderDetail> source2 = env
                .socketTextStream("hadoop102",8888)
                .map((MapFunction<String, OrderDetail>) value -> {
                    String[] s = value.split(",");
                    return new OrderDetail(s[0].trim(), s[1].trim(),s[2].trim(),Long.valueOf(s[3].trim()));
                })
                .assignTimestampsAndWatermarks(
                        //乱序流
                        WatermarkStrategy
                                .<OrderDetail>forBoundedOutOfOrderness(Duration.ZERO)    //设置乱序流延迟
                                .withTimestampAssigner((SerializableTimestampAssigner<OrderDetail>) (element, recordTimestamp) -> element.getTs())    //从数据源获取时间
                );

        source1
                .keyBy(OrderInfo::getOrderId)
                .intervalJoin(source2.keyBy(OrderDetail::getOrderId))
                .between(Time.seconds(-2),Time.seconds(3))
                .lowerBoundExclusive()  //排除下边界
                .upperBoundExclusive()  //排除上边界
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, String>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, ProcessJoinFunction<OrderInfo, OrderDetail, String>.Context ctx, Collector<String> out) {
                        out.collect(left+"--"+right);
                    }
                })
                .print();

        //启动程序执行
        env.execute();
    }
}
