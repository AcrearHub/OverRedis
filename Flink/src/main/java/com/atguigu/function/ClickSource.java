package com.atguigu.function;

import com.atguigu.bean.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 模拟用户的点击事件相关数据
 */
public class ClickSource implements SourceFunction<Event> {
    private static boolean isRunning = true;
    /**
     * 用于生成数据，通过SourceContext发射数据
     */
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        String [] users = {"赵四","刘能","谢广坤","谢大脚","Tom"};
        String [] urls = {"/home","/detail","/cart","/pay"};
        Random random = new Random();
        //通过循环，持续产生数据
        while(isRunning){
            //构造Event数据
            Event event = new Event(users[random.nextInt(users.length)], urls[random.nextInt(urls.length)], System.currentTimeMillis());
            //发射数据
            sourceContext.collect(event);
            //限制生成时间
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
