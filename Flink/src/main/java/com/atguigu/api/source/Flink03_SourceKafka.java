package com.atguigu.api.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class Flink03_SourceKafka {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行；从kafka读数据尽量设置和kafka分区数一致，效率最大化
        env.setParallelism(1);

        KafkaSource<String> aTrue = KafkaSource
                .<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop03:9092,hadoop104:9092")
                .setGroupId("flink-test")
//              .setDeserializer()  //针对kv的反序列化器设置
                .setValueOnlyDeserializer(new SimpleStringSchema())  //针对v的反序列化器设置
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))    //offset重置策略
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")    //设置自动提交
                .setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000")    //设置自动提交间隔
                .build();

        env.fromSource(aTrue, WatermarkStrategy.noWatermarks(),"kafka");

        //启动程序执行
        env.execute();
    }
}
