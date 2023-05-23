package com.atguigu.api.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Flink01_SourceCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Object> objectDataStreamSource = env.fromCollection(Arrays.asList(1,55,23,6));
        objectDataStreamSource.print();
        env.execute();
    }
}
