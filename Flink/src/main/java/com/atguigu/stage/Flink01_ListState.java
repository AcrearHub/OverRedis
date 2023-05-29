package com.atguigu.stage;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;


public class Flink01_ListState {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度
        env.setParallelism(2);

        //打开检查点，进行备份，有异常默认无限重启
        env.enableCheckpointing(5000L);

        env
                .socketTextStream("hadoop102",8888)
                .map(new MyMapFunction())
                .addSink(new SinkFunction<String>() {
                    @Override
                    public void invoke(String value, Context context) {
                        if (value.contains("x")){
                            throw new RuntimeException("有异常");
                        }
                        System.out.println(value);
                    }
                });

        //启动程序执行
        env.execute();
    }

    public static class MyMapFunction implements MapFunction<String,String>, CheckpointedFunction{
        //列表状态
        private ListState<String> dates;
        @Override
        public String map(String value) throws Exception {
            dates.add(value);
            return dates.get().toString();
        }

        /**
         * 用于状态的备份，伴随checkpoint周期性执行，不用手动管
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) {

        }

        /**
         * 用于算子状态初始化，算子创建时调用一次
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //获取算子状态的存储，在程序重启后拿到算子状态
            OperatorStateStore operatorStateStore = context.getOperatorStateStore();
            //获取算子状态，并赋值 -- ListState
            dates = operatorStateStore.getListState(new ListStateDescriptor<>("状态1", String.class));
            //获取算子状态，并赋值 -- UnionListState
            //dates = operatorStateStore.getUnionListState(new ListStateDescriptor<>("状态2", String.class));
        }
    }
}
