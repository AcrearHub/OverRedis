package com.atguigu.api.transform;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * 富函数：RichMapFunction、RichFilterFunction、RichReduceFunction......
 *  特点：拥有生命周期方法：open、close
 *       获取运行时上下文：RuntimeContext，可以获取到更多信息
 */
public class Flink06_RichFunction {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        ClickSource clickSource = new ClickSource();

        DataStreamSource<Event> test = env.addSource(clickSource);

        //map富函数测试
        test
                .map(new RichMapFunction<Event, String>() {

                    /**
                     * 算子开始时调用一次
                     */
                    @Override
                    public void open(Configuration parameters) {
                        System.out.println("=====我来辣=====");
                    }

                    @Override
                    public String map(Event event) {
                        return JSON.toJSONString(event);
                    }

                    /**
                     * 算子结束时调用一次
                     */
                    @Override
                    public void close() {
                        System.out.println("=====我走辣=====");
                    }
                })
                .print("map");

        //优雅关闭：测试周期方法close时需要优雅关闭
        new Thread(new Runnable() {
            final File f = new File("d:\\jinitaimei.txt");
            @Override
            public void run() {
                boolean isRunning = true;
                System.out.println("监控开启");
                while(isRunning){
                    if (f.exists()) {
                        clickSource.cancel();
                        isRunning = false;
                        System.out.println("程序关闭，监控结束");
                    }
                }
            }
        }).start();

        //启动程序执行
        env.execute();
    }
}
