package com.dubbo.simple.server;

import com.dubbo.simple.common.DemoService;

public class DemoServiceImpl implements DemoService {
    @Override
    public String sayHello(String name) {
        return "Hello " + name;
    }

    @Override
    public String sayGoodBye(String name) {
        return "GoodBye " + name;
    }

}
