package com.atguigu.api.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public class Flink03_SinkKafkaWithKey {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        //开启检查点
        env.enableCheckpointing(5000);

        KafkaSink<Event> build = KafkaSink
                .<Event>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(
                        //写入带Key的消息,需要自己实现序列化过程
                        (KafkaRecordSerializationSchema<Event>) (event, kafkaSinkContext, aLong) -> {
                            //按照key分组
                            byte[] key = event.getUser().getBytes(StandardCharsets.UTF_8);
                            byte[] value = JSON.toJSONString(event).getBytes(StandardCharsets.UTF_8);

                            return new ProducerRecord<>("topic_db", key, value);
                        }
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                //如果👆是精确一次，则必须设置事务ID：👇
                .setTransactionalIdPrefix("flink")
                .build();

        env
                .addSource(new ClickSource())
                .sinkTo(build);

        //启动程序执行
        env.execute();
    }
}
