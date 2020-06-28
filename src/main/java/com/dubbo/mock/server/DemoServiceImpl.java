package com.dubbo.mock.server;

import com.dubbo.mock.common.DemoService;

public class DemoServiceImpl implements DemoService {
    @Override
    public String sayHello(String name) {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String sayGoodBye(String name) {
        return "GoodBye " + name;
    }
}
