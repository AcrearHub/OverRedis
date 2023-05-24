package com.atguigu.api.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * 和Source对应
 * 旧方法：addSink(SinkFunction)
 * 新方法：sinkTo(Sink)
 */
public class Flink01_SinkFile {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        //开启检查点：使文件形成最终效果
        env.enableCheckpointing(5000);

        FileSink<String> build = FileSink
                .<String>forRowFormat(
                        new Path("D:\\Atguigu\\05_Code\\OverRedis\\OverRedis\\FlinkOutput"),
                        new SimpleStringEncoder<>(StandardCharsets.UTF_8.toString())
                )
                .withRollingPolicy(
                        DefaultRollingPolicy
                                .builder()
                                .withMaxPartSize(MemorySize.parse("1M"))  //文件最大大小：1M
                                .withRolloverInterval(Duration.ofSeconds(10)) //文件滚动时间间隔
                                .withInactivityInterval(Duration.ofSeconds(10))   //多久没有写入数据滚动一次
                                .build()
                )    //设置文件滚动策略
                .withOutputFileConfig(new OutputFileConfig("前缀", "后缀")) //设置文件前缀、后缀
                .withBucketCheckInterval(1000L)  //设置检查时间间隔
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH-mm"))   //设置文件分区名的时间格式
                .build();

        env
                .addSource(new ClickSource())
                .map(JSON::toJSONString)
                .sinkTo(build);

        //启动程序执行
        env.execute();
    }
}
