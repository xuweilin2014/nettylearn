package com.dubbo.stub.common;

import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;

public class DemoServiceStub implements DemoService{

    private static Logger logger = LoggerFactory.getLogger(DemoServiceStub.class);

    private DemoService demoService;

    public DemoServiceStub(DemoService demoService) {
        this.demoService = demoService;
    }

    @Override
    public String sayHello(String name) {
        logger.info("before sayHello executes, parameter is " + name);
        try{
            String s = demoService.sayHello("world");
            logger.info("after sayHello executes, result is " + name);
            return "okk";
        }catch (Throwable e){
            logger.warn("fail to execute");
            return null;
        }
    }

    @Override
    public String sayGoodBye(String name) {
        return null;
    }
}
