package com.dubbo.mock.common;

import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;

public class DemoServiceMock implements DemoService{

    private static Logger logger = LoggerFactory.getLogger(com.dubbo.simple.common.DemoServiceMock.class);

    @Override
    public String sayHello(String name) {
        System.out.println("mock executed");
        logger.warn("about to execute mock: " + com.dubbo.simple.common.DemoServiceMock.class.getSimpleName());
        return "mock: " + name;
    }

    @Override
    public String sayGoodBye(String name) {
        return null;
    }

}
