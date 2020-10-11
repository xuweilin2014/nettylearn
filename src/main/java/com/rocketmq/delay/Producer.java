package com.rocketmq.delay;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

public class Producer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        // 1.创建消息生产者 producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("delay-producer");
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
            Message msg = new Message("delay-topic", "Tag1", ("Hello World" + i).getBytes());
            msg.setDelayTimeLevel(2);
            // 5.发送消息
            SendResult result = producer.send(msg);
            // 发送状态
            SendStatus status = result.getSendStatus();

            System.out.println("发送结果: " + result);

            // 线程睡1秒
            TimeUnit.SECONDS.sleep(1);
        }

        // 6.关闭生产者 producer
        producer.shutdown();
    }

}
