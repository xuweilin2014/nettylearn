package com.dubbo.callback.consumer;

import com.dubbo.service.type.callback.CallbackListener;
import com.dubbo.service.type.callback.CallbackService;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.concurrent.CountDownLatch;

public class Consumer {

    public static void main(String[] args) throws Exception {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:consumer.xml");
        context.start();
        CallbackService callbackService = (CallbackService) context.getBean("callbackService"); // 获取远程服务代理
        callbackService.addListener("hello-world", new CallbackListener() {
            @Override
            public void changed(String msg) {
                System.out.println("callback " + msg);
            }
        }); // 执行远程方法
        new CountDownLatch(1).await();
    }

}
