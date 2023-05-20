package com.atguigu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建Flink环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从数据源读数据
        DataSource<String> dataSource = env.readTextFile("D:\\Atguigu\\05_Code\\OverRedis\\FlinkInput\\word.txt");

        //调用Flink API进行转换处理
        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = dataSource.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (words, collector) -> {
            String[] s1 = words.split(" ");
            for (String s2 : s1) {
                collector.collect(Tuple2.of(s2, 1));
            }
        }).groupBy(0);

        //汇总、输出结果
        AggregateOperator<Tuple2<String, Integer>> sum = tuple2UnsortedGrouping.sum(2);
        sum.print();
    }
}
