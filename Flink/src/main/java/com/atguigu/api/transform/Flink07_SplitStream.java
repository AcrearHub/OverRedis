package com.atguigu.api.transform;

import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 分流操作，利用process算子
 */
public class Flink07_SplitStream {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        DataStreamSource<Event> test = env.addSource(new ClickSource());

        //分流
        OutputTag<Event> co = new OutputTag<Event>("外国人"){};    //声明OutputTag一定要匿名内部类

        SingleOutputStreamOperator<Event> process = test
                .process(new ProcessFunction<Event, Event>() {
                    @Override
                    public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) {
                        String user = value.getUser();
                        if ("Tom".equals(user)) {
                            ctx.output(co, value); //侧流
                        } else {
                            out.collect(value); //主流
                        }
                    }
                });

        process.print("主流：中国人");
        process.getSideOutput(co).print("侧流：外国人");

        //启动程序执行
        env.execute();
    }
}
