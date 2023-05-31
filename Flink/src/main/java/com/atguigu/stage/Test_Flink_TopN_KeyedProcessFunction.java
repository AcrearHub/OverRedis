package com.atguigu.stage;

import com.atguigu.bean.Event;
import com.atguigu.bean.URLViewCount;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

/**
 * 需求：url每10s每TopN
 */
public class Test_Flink_TopN_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> test = env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((event, ts) -> event.getTs())
                );
        test.print("source");

        SingleOutputStreamOperator<URLViewCount> aggregate = test
                .map(Event::getUrl)
                .keyBy(url -> url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .aggregate(
                        new AggregateFunction<String, Long, URLViewCount>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(String value, Long accumulator) {
                                return accumulator + 1;
                            }

                            @Override
                            public URLViewCount getResult(Long accumulator) {
                                return new URLViewCount(null, null, null, accumulator);
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<URLViewCount, URLViewCount, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<URLViewCount, URLViewCount, String, TimeWindow>.Context context, Iterable<URLViewCount> elements, Collector<URLViewCount> out) {
                                Long start = context.window().getStart();
                                Long end = context.window().getEnd();
                                URLViewCount urlViewCount = elements.iterator().next();
                                urlViewCount.setStart(start);
                                urlViewCount.setEnd(end);
                                out.collect(urlViewCount);
                            }
                        }
                );
        aggregate.print("count");

        aggregate
                .keyBy(URLViewCount::getEnd)
                .process(
                        new KeyedProcessFunction<Long, URLViewCount, String>() {
                            //维护数据
                            private ListState<URLViewCount> listState;

                            //标记是否有定时器
                            private ValueState<Boolean> valueState;
                            @Override
                            public void open(Configuration parameters) {
                                ListStateDescriptor<URLViewCount> listStateDescriptor = new ListStateDescriptor<>("listState", URLViewCount.class);
                                listState = getRuntimeContext().getListState(listStateDescriptor);

                                ValueStateDescriptor<Boolean> valueStateDescriptor = new ValueStateDescriptor<>("valueState", Boolean.class);
                                valueState = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public void processElement(URLViewCount value, KeyedProcessFunction<Long, URLViewCount, String>.Context ctx, Collector<String> out) throws Exception {
                                listState.add(value);
                                if (valueState.value() == null || !valueState.value()){
                                    //注册1s的定时器
                                    ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1000L);
                                }
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<Long, URLViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                                //对所有数据排序
                                ArrayList<URLViewCount> arrayList = new ArrayList<>();
                                for (URLViewCount urlViewCount : listState.get()) {
                                    arrayList.add(urlViewCount);
                                }
                                StringBuilder stringBuilder = new StringBuilder();
                                arrayList.sort((o1, o2) -> Long.compare(o2.getCount(), o1.getCount()));
                                for (int i = 0; i < Math.min(arrayList.size(), 3); i++) {
                                    stringBuilder.append("No").append(i + 1).append(" ").append(arrayList.get(i)).append("\n");
                                }
                                out.collect(stringBuilder.toString());
                            }
                        }
                )
                .print("TopN\n");

        //启动程序执行
        env.execute();
    }
}
