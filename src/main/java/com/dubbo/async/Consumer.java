package com.dubbo.async;

import com.alibaba.dubbo.rpc.RpcContext;
import com.dubbo.simple.common.DemoService;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.concurrent.Future;

public class Consumer {

    public static void main(String[] args) {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:consumer.xml");
        context.start();
        DemoService demoService = (DemoService)context.getBean("demoService"); // 获取远程服务代理
        String hello = demoService.sayHello("world"); // 执行远程方法
        Future<Object> future = RpcContext.getContext().getFuture();
        System.out.println( hello ); // 显示调用结果
    }

}
