package com.atguigu.api.env;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 演示流环境
 */
public class Flink01_Env {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();

        StreamExecutionEnvironment remoteEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("localhost",8888,"path/to/jarFile.jar");

        env.execute();
        localEnvironment.execute();
        remoteEnvironment.execute();
    }
}
