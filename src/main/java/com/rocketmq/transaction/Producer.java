package com.rocketmq.transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.TimeUnit;

public class Producer {

    public static void main(String[] args) throws Exception {
        //创建消息生产者
        TransactionMQProducer producer = new TransactionMQProducer("transaction-producer");
        producer.setNamesrvAddr("127.0.0.1:9870;127.0.0.1:9876");
        //生产者这是监听器
        producer.setTransactionListener(new TransactionListener() {
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                if (StringUtils.equalsAny("TAGA", message.getTags())){
                    return LocalTransactionState.COMMIT_MESSAGE;
                }else if (StringUtils.equalsAny("TAGB", message.getTags())){
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }else if (StringUtils.equalsAny("TAGC", message.getTags())){
                    return LocalTransactionState.UNKNOW;
                }
                return LocalTransactionState.UNKNOW;
            }

            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                System.out.println("接收到的消息：" + messageExt.getTags());
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

        String[] tags = new String[]{"TAGA", "TAGB", "TAGC"};
        //启动消息生产者
        producer.start();
        for (int i = 0; i < 3; i++) {
            try {
                Message msg = new Message("transaction-topic", tags[i], ("Hello RocketMQ " + i).getBytes());
                SendResult sendResult = producer.sendMessageInTransaction(msg, null);
                System.out.printf("%s%n", sendResult);
                TimeUnit.SECONDS.sleep(1);
            } catch (MQClientException e) {
                e.printStackTrace();
            }
        }
    }

}
