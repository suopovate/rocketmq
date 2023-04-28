/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.consumer;

import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.*;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class VateDefaultMQPushConsumerTest {

    private String consumerGroup;
    private String topic = "FooBar";
    private String brokerName = "BrokerA";
    private MQClientInstance mQClientFactory;

    @Test
    public void testDefaultMqPushConsumer() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        String group = "vateTest";
        String namesrvAddr = "127.0.0.1:9876";
        String topic = "vtopic_broadcast";

        DefaultMQPushConsumer consumer1 = new DefaultMQPushConsumer(group);
        DefaultMQPushConsumer consumer2 = new DefaultMQPushConsumer(group);
        // 📢广播模式
        consumer1.setNamesrvAddr(namesrvAddr);
//        consumer1.setMessageModel(MessageModel.BROADCASTING);
        consumer1.setInstanceName("vconsumer1");
        consumer2.setNamesrvAddr(namesrvAddr);
//        consumer2.setMessageModel(MessageModel.BROADCASTING);
        consumer2.setInstanceName("vconsumer2");

        consumer1.subscribe(topic, "*");
        consumer1.setMessageListener((MessageListenerOrderly) (msgs, context) -> {
            System.out.println("consumer1: " + msgs);
            return null;
        });
        consumer2.subscribe(topic, "*");
        consumer2.setMessageListener((MessageListenerOrderly) (msgs, context) -> {
            System.out.println("consumer2: " + msgs);
            return null;
        });

        consumer1.start();
        consumer2.start();
        DefaultMQProducer producer = new DefaultMQProducer(group);
        producer.setNamesrvAddr(namesrvAddr);
        producer.start();
        while (true) {
            Thread.sleep(1000 * 2);
            producer.send(makeMessage(topic));
        }
    }

    @Test
    public void testDefaultMqPullLiteConsumer() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        String group = "vateTest";
        String namesrvAddr = "127.0.0.1:9876";
        String topic = "vtopic_broadcast_10";

//        DefaultMQPushConsumer consumer1 = new DefaultMQPushConsumer(group);
//        consumer1.setNamesrvAddr(namesrvAddr);
//        consumer1.setInstanceName("vconsumer1");
//        consumer1.subscribe(topic, "*");
//        consumer1.setMessageListener((MessageListenerOrderly) (msgs, context) -> {
//            System.out.println("consumer1: " + msgs);
//            return null;
//        });
//        consumer1.start();
        String vconsumer1 = "vconsumer1";

        DefaultLitePullConsumer consumer2 = new DefaultLitePullConsumer(group);
        consumer2.setNamesrvAddr(namesrvAddr);
        consumer2.setInstanceName(vconsumer1);
        consumer2.subscribe(topic, "*");
//        consumer2.setMessageQueueListener((topic1, mqAll, mqDivided) -> {
//            // 这里其实应该搞一个新的集合好点
//            consumer2.assign();
//        });
//        consumer2.start();
//        new Thread(() -> {
//            try {
//                while (true){
//                    Thread.sleep(1000 * 3);
//                    List<MessageExt> messageExts = consumer2.poll();
//                    System.out.println("接收消息：" + JSON.toJSONString(messageExts));
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }).start();

        DefaultMQProducer producer = new DefaultMQProducer(group);
        producer.setInstanceName(vconsumer1);
        producer.setNamesrvAddr(namesrvAddr);
        producer.start();
//        while (true) {
            Thread.sleep(1000 * 2);
            producer.send(makeMessage(topic));
//        }
    }

    private Message makeMessage(String topic) {
        return new Message(topic, ("testMsg" + System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
    }
}
