package com.rocketmq.base.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class Consumer {

    public static void main(String[] args) throws MQClientException {
        // 1.创建消费者 Consumer，指定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-group1");
        // 2.指定 NameServer 地址
        consumer.setNamesrvAddr("127.0.0.1:9870;127.0.0.1:9876");
        // 3.消费消息有两种模式：广播模式和负载均衡模式，不设置的话，默认是负载均衡模式
        consumer.setMessageModel(MessageModel.BROADCASTING);
        // 4.订阅主题 topic 和 tag
        consumer.subscribe("nettylearn-mq", MessageSelector.byTag("Tag2"));
        // 5.设置回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println(new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 6.启动消费者
        consumer.start();
    }

}
