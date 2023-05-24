package com.atguigu.api.transform;

import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * process算子介绍
 * （代码无法执行）
 */
public class Flink05_ProcessIntroduce {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        DataStreamSource<Event> test = env.addSource(new ClickSource());

        //没有keyBy
        SingleOutputStreamOperator<String> process = test
                .process(new ProcessFunction<Event, String>() {
                    /**
                     *
                     * @param value 当前数据
                     * @param ctx 上下文，可获取各种信息；一般通过它进行对侧流的输出
                     * @param out 收集器，可以收集信息并发射
                     */
                    @Override
                    public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) {
                        ctx.timerService(); //时间服务，可注册定时器
                        ctx.output(new OutputTag<>("test"),value);   //侧流输出
                        out.collect("test");  //主流输出
                        getRuntimeContext();    //状态编程
                    }
                });

        process.getSideOutput(new OutputTag<>("test")).print();    //接收侧流数据
        process.print();    //接收主流数据

        //经过keyBy
        test
                .keyBy((KeySelector<Event, String>) Event::getUser)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) {
                        ctx.getCurrentKey();    //获取当前的key，其余方法同上
                    }
                })
                .print();

        //。。。

        //启动程序执行
        env.execute();
    }
}
