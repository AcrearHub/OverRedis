package com.atguigu.api.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;

import java.util.UUID;

/**
 * 随机生成UUID字符串
 */
public class Flink04_SourceDataGenerator {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        DataGeneratorSource<String> stringDataGeneratorSource = new DataGeneratorSource<>(
                new RandomGenerator<String>() {
                    @Override
                    public String next() {
                        return UUID.randomUUID().toString();    //随机生成UUID
                    }
                },1,10L
        );
        env
                .addSource(stringDataGeneratorSource)
                .returns(String.class)
                .print();

        //启动程序执行
        env.execute();
    }
}
