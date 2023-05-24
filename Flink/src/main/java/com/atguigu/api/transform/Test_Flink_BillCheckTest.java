package com.atguigu.api.transform;

import com.atguigu.bean.AppEvent;
import com.atguigu.bean.ThirdPartEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * 需求：实现一个实时对账的需求，也就是app的支付操作和第三方的支付操作的一个双流Join
 *      最终只输出对账成功的数据，暂时忽略数据的延迟
 */
public class Test_Flink_BillCheckTest {
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
                .filter((FilterFunction<AppEvent>) s -> "pay".equals(s.getEventType()));

        //从9999端口读取ThirdPartEvent：e.g：order-1,pay,Alipay,3000
        SingleOutputStreamOperator<ThirdPartEvent> thirdPart = env
                .socketTextStream("hadoop102", 9999)
                .map((MapFunction<String, ThirdPartEvent>) s -> {
                    String[] split = s.split(",");
                    return new ThirdPartEvent(split[0].trim(), split[1].trim(), split[2].trim(), Long.valueOf(split[3].trim()));
                });

        //合流
        app
                .connect(thirdPart)
                .process(new CoProcessFunction<AppEvent, ThirdPartEvent, String>() {
                    private final Set<String> appCache = new HashSet<>();
                    private final Set<String> thirdPartCache = new HashSet<>();
                    @Override
                    public void processElement1(AppEvent value, CoProcessFunction<AppEvent, ThirdPartEvent, String>.Context ctx, Collector<String> out) {
                        if (thirdPartCache.contains(value.getOrderId())){
                            out.collect(value.getOrderId()+"对账成功");
                            appCache.remove(value.getOrderId());
                        }else {
                            appCache.add(value.getOrderId());
                        }
                    }

                    @Override
                    public void processElement2(ThirdPartEvent value, CoProcessFunction<AppEvent, ThirdPartEvent, String>.Context ctx, Collector<String> out) {
                        if (appCache.contains(value.getOrderId())){
                            out.collect(value.getOrderId()+"对账成功");
                            thirdPartCache.remove(value.getOrderId());
                        }else {
                            thirdPartCache.add(value.getOrderId());
                        }
                    }
                })
                .print();

        //启动程序执行
        env.execute();
    }
}
