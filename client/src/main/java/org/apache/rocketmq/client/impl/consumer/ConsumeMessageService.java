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
package org.apache.rocketmq.client.impl.consumer;

import java.util.List;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;

/**
 * rmq 的 消费端消息的拉取逻辑：依据位点信息，构造请求去拉一批，拉完一批，就会立马，构建下一个请求去拉第二批，直到没消息了，就可能停一会儿，相当于轮询，当然也有限流相关的控制。
 *
 * 在拉到消息后，基于你是顺序还是并发消费，就会有不同的处理方式
 *
 * 首先，不管是哪种消费，都会先把拉下来的消息 存储到一个缓冲区 ，里面缓存了所有这个队列现在为止拉下来的，待消费的消息，是根据位点排序的
 *
 * 然后根据顺序和并发消费类型，构造消息消费任务，丢给不同的 消息消费服务 线程池。
 *
 * 然后 如果是 顺序消费，那么 顺序消费任务执行时，首先会给mq加锁，防止同时有多个线程消费这个消息队列的消息，然后顺序消费线程，从 缓冲区里边 按位点顺序来消费消息，从前往后的消费，消费失败就卡住，过会从失败的点继续（这里还有个错误重试的 还没看完 不影响主流程理解）。
 *
 * 如果是并发消费，那它完全是不用缓冲区的消息的（也不是完全不管，不过也只是从里面拿位点信息最统计而已），并发消费是每拉到一批消息，就直接把这批消息，拆成多个消费任务，丢个消息消费服务，然后可能会有多个线程异步去处理那些消息，无序
 *
 * 客户端拉到消息后，丢给本服务来消费。
 */
public interface ConsumeMessageService {
    void start();

    void shutdown(long awaitTerminateMillis);

    void updateCorePoolSize(int corePoolSize);

    void incCorePoolSize();

    void decCorePoolSize();

    int getCorePoolSize();

    ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg, final String brokerName);

    void submitConsumeRequest(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final boolean dispathToConsume);
}
