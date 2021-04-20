package org.apache.rocketmq.client.impl.consumer;

import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

public class LitePullConsumerBug {

    // ./mqadmin updateTopic -n 127.0.0.1:9876 -t TopicTest -r 1 -w 1 -c DefaultCluster
    private static final String NAME_SERVER = "127.0.0.1:9876";
    private static final String TOPIC = "TopicTest";
    private static final String CONSUMER_GROUP = "test_group";

    private DefaultLitePullConsumer consumer;

    private static void send() {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("producer_group");
            producer.setNamesrvAddr(NAME_SERVER);
            producer.start();
            final Message msg = new Message(TOPIC, new byte[] {1});
            SendResult result = producer.send(msg);
            System.out.println(result);
            producer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        send();

        final LitePullConsumerBug pullConsumer = new LitePullConsumerBug();
        pullConsumer.startConsumer();

        Thread.sleep(2000);
        LogUtil.log("start consumer 2");

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(CONSUMER_GROUP);
                    consumer.setInstanceName("1");
                    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
                    consumer.setNamesrvAddr(NAME_SERVER);
                    consumer.setAutoCommit(false);
                    consumer.start();
                    consumer.subscribe(TOPIC, "*");
                    Thread.sleep(2000);
                    consumer.shutdown();
                    Thread.sleep(5000);
                    send();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public void startConsumer() throws Exception {
        consumer = new DefaultLitePullConsumer(CONSUMER_GROUP);
        consumer.setInstanceName("2");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setNamesrvAddr(NAME_SERVER);
        consumer.setAutoCommit(false);

        consumer.start();
        consumer.subscribe(TOPIC, "*");

        new Thread(new Runnable() {
            @Override
            public void run() {
                poll();
            }
        }).start();
    }

    private void poll() {
        while (true) {
            List<MessageExt> list = consumer.poll(100);
            if (list == null || list.size() == 0) {
                continue;
            }
            for (MessageExt ext : list) {
                System.out.println("receive msg: " + ext.getMsgId());
            }
            consumer.commitSync();
        }
    }
}
