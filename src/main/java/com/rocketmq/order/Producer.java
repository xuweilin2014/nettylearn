package com.rocketmq.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class Producer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("order-producer");
        // 指定 Nameserver 地址
        producer.setNamesrvAddr("127.0.0.1:9870;127.0.0.1:9876");
        // 启动 producer
        producer.start();
        List<OrderStep> orders = OrderStep.buildOrders();

        for (OrderStep order : orders) {
            // 创建消息对象，指定主题 Topic、Tag 和消息体
            /*
             * 参数一：消息主题 Topic
             * 参数二：消息 Tag
             * 参数三：消息内容
             */
            Message msg = new Message("order-topic", "tag1", order.toString().getBytes());
            // 发送消息
            SendResult result = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    long orderId = (long) arg;
                    return mqs.get((int) (orderId % mqs.size()));
                }
            }, order.getOrderId());

            System.out.println("发送结果: " + result);

            // 线程睡1秒
            TimeUnit.SECONDS.sleep(1);
        }

        // 关闭生产者 producer
        producer.shutdown();
    }

}
