package com.dubbo.simple.common;

import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.dubbo.simple.common.DemoService;

public class DemoServiceMock implements DemoService {
    private static Logger logger = LoggerFactory.getLogger(DemoServiceMock.class);

    @Override
    public String sayHello(String name) {
        System.out.println("mock executed");
        logger.warn("about to execute mock: " + DemoServiceMock.class.getSimpleName());
        return "mock: " + name;
    }

    @Override
    public String sayGoodBye(String name) {
        return null;
    }
}
