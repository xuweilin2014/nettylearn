package com.xu.test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Test {
    public static void main(String[] args) {
        /*使用Executors工具快速构建对象*//*
        ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor();
        System.out.println("3秒后开始执行计划线程池服务..." + new Date());
        *//*每间隔4秒执行一次任务*//*
        scheduledExecutorService.scheduleWithFixedDelay(new MyRunnable(),
                3, 4, TimeUnit.SECONDS);*/

        Map<String, String> map = new HashMap<>();
        System.out.println(map.get("String"));
    }

    static class MyRunnable implements Runnable{

        private AtomicInteger atomicInteger = null;
        private Random random = null;

        public MyRunnable() {
            atomicInteger = new AtomicInteger(0);
            random = new Random();
        }

        @Override
        public void run() {
            try {
                String threadName = Thread.currentThread().getName();
                System.out.println("1-任务执行开始:" + new Date() + ":" + threadName);
                /*使用随机延时[0-3]秒来模拟执行任务*/
                int sleepNumber = random.nextInt(10);
                TimeUnit.SECONDS.sleep(sleepNumber);
                /*if (atomicInteger.getAndAdd(1) == 3) {

                    int error = 10 / 0;
                }*/
                System.out.println("2-任务执行完毕:" + new Date() + ":" + threadName + " , 睡眠了" + sleepNumber + "秒");
                System.out.println();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
