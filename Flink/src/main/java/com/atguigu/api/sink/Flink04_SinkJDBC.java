package com.atguigu.api.sink;

import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class Flink04_SinkJDBC {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        SinkFunction<Event> root = JdbcSink.sink(
                "INSERT INTO clicks (user,url,ts) VALUES(?,?,?)",
                (JdbcStatementBuilder<Event>) (preparedStatement, event) -> {
                    //占位符赋值
                    preparedStatement.setString(1, event.getUser());
                    preparedStatement.setString(2, event.getUrl());
                    preparedStatement.setLong(3, event.getTs());
                },
                JdbcExecutionOptions
                        .builder()
                        .withMaxRetries(3)//重试最大次数
                        .withBatchSize(5)//每批次写入多少数据
                        .withBatchIntervalMs(5000L)//过多久写一次，即使没数据
                        .build()
                ,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test")
                        .withUsername("root")
                        .withPassword("000000")
                        .build()
        );

        env
                .addSource(new ClickSource())
                .addSink(root);

        //启动程序执行
        env.execute();
    }
}
