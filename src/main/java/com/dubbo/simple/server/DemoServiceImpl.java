package com.dubbo.simple.server;

import com.alibaba.dubbo.rpc.RpcException;
import com.dubbo.simple.common.DemoService;

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
