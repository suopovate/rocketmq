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
package org.apache.rocketmq.client.consumer.listener;

/**
 * 这个是我们消费者实现，可以返回的一个状态值，如成功、或者需要挂起队列一会儿
 *
 * 注意这里很有意思，它不是针对某个消息进行延迟，而是整个队列都延迟，为什么？因为是顺序消费模式，单个队列的消息，前后消费顺序严格一致。
 */
public enum ConsumeOrderlyStatus {
    /**
     * Success consumption
     */
    SUCCESS,
    /**
     * Rollback consumption(only for binlog consumption)
     */
    @Deprecated
    ROLLBACK,
    /**
     * Commit offset(only for binlog consumption)
     */
    @Deprecated
    COMMIT,
    /**
     * Suspend current queue a moment
     */
    SUSPEND_CURRENT_QUEUE_A_MOMENT;
}
