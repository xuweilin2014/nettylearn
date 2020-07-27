package com.dubbo.notify.client;

import com.dubbo.service.common.NotifyService;
import com.dubbo.simple.common.DemoService;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Consumer {

    public static void main(String[] args) {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:consumer.xml");
        context.start();
        NotifyService notifyService = (NotifyService)context.getBean("notifyService"); // 获取远程服务代理
        String hello = notifyService.sayHello(4); // 执行远程方法
        System.out.println( hello ); // 显示调用结果
    }

}
