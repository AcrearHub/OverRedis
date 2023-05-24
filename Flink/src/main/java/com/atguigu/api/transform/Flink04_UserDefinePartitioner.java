package com.atguigu.api.transform;

import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义分区(按照当前数据的key计算分区号,明确当前数据的key)
 */
public class Flink04_UserDefinePartitioner {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：这里至少为2
        env.setParallelism(2);

        DataStreamSource<Event> test = env.addSource(new ClickSource());

        test
                .partitionCustom((Partitioner<String>) (s, i) -> {
            if (s.equals("Tom")){
                return 0;
            }else {
                return 1;
            }
        }, (KeySelector<Event, String>) Event::getUser)
                .print();

        //启动程序执行
        env.execute();
    }
}
