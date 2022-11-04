package com.atguigu.datastreamapi.timeandwindow;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class UrlViewCount {
    public String url;
    public Long count;
    public Long window_start;
    public Long window_end;

    public UrlViewCount() {
    }

    public UrlViewCount(String url, Long count, Long window_start, Long window_end) {
        this.url = url;
        this.count = count;
        this.window_start = window_start;
        this.window_end = window_end;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", window_start=" + window_start +
                ", window_end=" + window_end +
                '}';
    }
}
