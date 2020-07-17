package com.dubbo.injvm;

import com.dubbo.simple.common.DemoService;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class InjvmDemo {

    public static void main(String[] args) {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:consumer.xml");
        context.start();

        DemoService demoService = (DemoService) context.getBean("demoService");
        String s = demoService.sayHello("world");
        System.out.println(s);
    }

}
