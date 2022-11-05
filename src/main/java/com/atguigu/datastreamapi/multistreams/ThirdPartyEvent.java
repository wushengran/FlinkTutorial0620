package com.atguigu.datastreamapi.multistreams;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class ThirdPartyEvent {
    public String orderId;
    public String eventType;
    public String platform;
    public Long timestamp;

    public ThirdPartyEvent() {
    }

    public ThirdPartyEvent(String orderId, String eventType, String platform, Long timestamp) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.platform = platform;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "ThirdPartyEvent{" +
                "orderId='" + orderId + '\'' +
                ", platform='" + platform + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
