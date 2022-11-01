package com.atguigu.datastreamapi.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class ClickEventSource implements SourceFunction<Event> {
    private boolean flag = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random();

        // 定义数据随机选择的范围
        String[] users = {"Alice", "Bob", "Cary", "Mary"};
        String[] urls = {"./home", "./prod?id=1", "./prod?id=2", "./cart", "./fav"};

        // 不断随机生成数据
        while (flag){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long ts = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user, url, ts));

            // 间隔1s
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
