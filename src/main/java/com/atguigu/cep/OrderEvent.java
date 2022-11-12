package com.atguigu.cep;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class OrderEvent {
    public String userId;
    public String orderId;
    public String eventType;
    public Long ts;

    public OrderEvent() {
    }

    public OrderEvent(String userId, String orderId, String eventType, Long ts) {
        this.userId = userId;
        this.orderId = orderId;
        this.eventType = eventType;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "userId='" + userId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", ts=" + ts +
                '}';
    }
}
