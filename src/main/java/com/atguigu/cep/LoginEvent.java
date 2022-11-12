package com.atguigu.cep;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial0620
 * <p>
 * Created by  wushengran
 */

public class LoginEvent {
    public String userId;
    public String ip;
    public String eventType;
    public Long ts;

    public LoginEvent() {
    }

    public LoginEvent(String userId, String ip, String eventType, Long ts) {
        this.userId = userId;
        this.ip = ip;
        this.eventType = eventType;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", ip='" + ip + '\'' +
                ", eventType='" + eventType + '\'' +
                ", ts=" + ts +
                '}';
    }
}
