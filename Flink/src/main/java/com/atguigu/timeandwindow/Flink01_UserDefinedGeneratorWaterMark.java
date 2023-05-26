package com.atguigu.timeandwindow;

import com.atguigu.bean.Event;
import com.atguigu.function.ClickSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 代码演示：自定义水位线策略：
 * 1、可用在每个算子后生成水位线，建议在靠近数据源处生成水位线（尽早生成）
 *      assignTimestampsAndWatermarks：
 *          WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context)  -   创建水位线
 *          default TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) - 对时间进行提取
 */
public class Flink01_UserDefinedGeneratorWaterMark {
    public static void main(String[] args) throws Exception {
        //创建流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度：不设置默认为全并行度；1为单线程执行
        env.setParallelism(1);

        DataStreamSource<Event> test = env.addSource(new ClickSource());

        //进行转换处理：窗口计算
        test.assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {
            /**
             * 生成水位线
             * @param context
             * @return
             */
            @Override
            public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<Event>() {

                    private final Long delay = 2000L; //2s的乱序流延迟
                    private Long mTs = Long.MIN_VALUE + delay + 1;

                    /**
                     * 每条数据一个水位线，若使用周期性生成水位线的方法，这里只负责维护已到数据的最大时间即可
                     * @param event
                     * @param eventTimestamp    由于createTimestampAssigner已经重写，所以这里=event.getTs
                     * @param output    发射水位线
                     */
                    @Override
                    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
//                      output.emitWatermark(new Watermark(eventTimestamp));//有序流水位线的生成

                        mTs = Math.max(mTs,eventTimestamp);
//                      output.emitWatermark(new Watermark(mTs-delay-1));//乱序流水位线的生成
                    }

                    /**
                     * 周期性生成水位线，默认200ms；不管有序还是乱序，都使用同一种方式发射即可
                     * @param output    发射水位线
                     */
                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(mTs-delay-1));
                    }
                };
            }

            /**
             * 从数据源获取时间
             * @param context
             * @return
             */
            @Override
            public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return (SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.getTs();
            }
        });

        //启动程序执行
        env.execute();
    }
}
