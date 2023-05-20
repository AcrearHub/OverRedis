package com.atguigu;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 批处理（过时）
 * 1、创建Flink环境
 * 2、从数据源读数据
 * 3、调用API进行处理
 * 4、汇总、输出结果
 */
public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        //1、创建Flink环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2、从数据源读数据
        DataSource<String> dataSource = env.readTextFile("D:\\Atguigu\\05_Code\\OverRedis\\OverRedis\\FlinkInput\\word.txt");

        //3、调用Flink API进行转换处理
        dataSource
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (words, collector) -> {
                    //根据空格来拆分
                    String[] s1 = words.split(" ");
                    for (String s2 : s1) {
                        //调用of方法来将元素传入，返回Tuple类型值
                        collector.collect(Tuple2.of(s2, 1));
                    }
                })
                //由于使用Lambda，导致产生泛型擦除问题，需再次指定泛型
                .returns(new TypeHint<Tuple2<String, Integer>>() {})
                /* 传入值：
                KeySelector：任意类型都可以使用该方法，通过getKey方法来返回指定的key.
                int：如果当前类型是Tuple,使用该方法，指定Tuple中的第几个元素作为key.
                String：如果当前类型是P0J0(Bean),使用该方法，指定P0J0中的哪个属性作为key
                */
                .groupBy(0)
        //4、汇总、输出结果
                .sum(1)
                .print();
    }
}
