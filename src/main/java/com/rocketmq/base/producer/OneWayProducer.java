package com.rocketmq.base.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

public class OneWayProducer {

    public static void main(String[] args) throws Throwable {
        // 1.创建消息生产者 producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("provider-group1");
        // 2.指定 Nameserver 地址
        producer.setNamesrvAddr("127.0.0.1:9870;127.0.0.1:9876");
        // 3.启动 producer
        producer.start();

        for (int i = 0; i < 10; i++) {
            // 4.创建消息对象，指定主题 Topic、Tag 和消息体
            /*
             * 参数一：消息主题 Topic
             * 参数二：消息 Tag
             * 参数三：消息内容
             */
            Message msg = new Message("nettylearn-mq", "Tag4", ("Hello World" + i).getBytes());
            // 5.发送消息
            producer.sendOneway(msg);

            // 线程睡1秒
            TimeUnit.SECONDS.sleep(1);
        }

        // 6.关闭生产者 producer
        producer.shutdown();
    }

}
