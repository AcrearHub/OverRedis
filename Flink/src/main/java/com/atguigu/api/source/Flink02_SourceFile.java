package com.atguigu.api.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_SourceFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> build = FileSource
                .<String>forRecordStreamFormat(new TextLineInputFormat(), new Path("D:\\Atguigu\\05_Code\\OverRedis\\OverRedis\\FlinkInput\\word.txt"))
                .build();
        env
                .fromSource(build, WatermarkStrategy.noWatermarks(),"file")
                .print();

        env.execute();
    }
}