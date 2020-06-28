package com.dubbo.stub.server;

import com.dubbo.stub.common.DemoService;

public class DemoServiceImpl implements DemoService {
    @Override
    public String sayHello(String name) {
        return "hello " + name;
    }

    @Override
    public String sayGoodBye(String name) {
        return "GoodBye " + name;
    }

}
