package com.atguigu.api.source;

import com.atguigu.function.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * 测试自定义数据源
 * 关于优雅关闭：在execute之前，开启另一个线程来监控本程序，修改控制循环的变量
 */
public class Flink05_SourceUserDefine {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        ClickSource clickSource = new ClickSource();

        env
                .addSource(new ClickSource())
                .print();

        //优雅关闭：若d盘下有鸡你太美.txt，则结束所有进程
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
