package com.atguigu.api.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 输出到kafka
 */
public class Flink02_SinkKafka {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        //开启检查点
        env.enableCheckpointing(5000);

        KafkaSink<String> build = KafkaSink
                .<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .builder()
                                .setTopic("topic_db") //设置主题
                                .setValueSerializationSchema(new SimpleStringSchema())  //设置序列化
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                //如果👆是精确一次，则必须设置事务ID：👇
                .setTransactionalIdPrefix("flink")
                .build();

        env
                .addSource(new ClickSource())
                .map(JSON::toJSONString)
                .sinkTo(build);

        //启动程序执行
        env.execute();
    }
}
