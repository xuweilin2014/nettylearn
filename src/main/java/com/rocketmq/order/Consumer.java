package com.rocketmq.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class Consumer {

    public static void main(String[] args) throws MQClientException {
        // 1.创建消费者 Consumer，指定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("order-consumer");
        // 2.指定 NameServer 地址
        consumer.setNamesrvAddr("127.0.0.1:9870;127.0.0.1:9876");
        // 4.订阅主题 topic 和 tag
        consumer.subscribe("order-topic", "tag1");
        // 5.设置回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println("线程名称：【" + Thread.currentThread().getName() + "】：" + new String(msg.getBody()));
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }

        });
        // 6.启动消费者
        consumer.start();
    }

}
