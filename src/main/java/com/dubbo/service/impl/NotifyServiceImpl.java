package com.dubbo.service.impl;

import com.dubbo.service.type.NotifyService;

public class NotifyServiceImpl implements NotifyService {

    @Override
    public String sayHello(int id) {
        if (id > 10)
            throw new RuntimeException("exception from sayHello: too large id");
        return "demo" + id;
    }

}
