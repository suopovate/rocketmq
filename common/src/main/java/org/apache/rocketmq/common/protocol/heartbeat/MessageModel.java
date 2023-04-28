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

/**
 * $Id: MessageModel.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.heartbeat;

/**
 * Message model
 * 总结一下：这两个东西的意义，是决定，在同一个消费者组内的消费者 消费消息的模式，即消费哪些消息。
 *
 * 广播模式：所有消费者都消费所有队列，消费者，（应该)永远都是从最新的数据开始消费(假如是新加入的，已加入的，按照你本地记录的位点正常拉取各个mq的消息)。
 *
 * 集群模式：所有消费者，均分mq，每个消费者获取部分mq的消费权，然后bk端需要记录topic下每个mq在当前消费者组的消费进度
 *         （注意这里，不是消费者是消费者组的mq，因为mq不是专属某个消费者，消费者只是一个可动态的逻辑概念，真正的实体其实是消费者组）。
 */
public enum MessageModel {
    /**
     * broadcast
     */
    BROADCASTING("BROADCASTING"),
    /**
     * clustering
     */
    CLUSTERING("CLUSTERING");

    private String modeCN;

    MessageModel(String modeCN) {
        this.modeCN = modeCN;
    }

    public String getModeCN() {
        return modeCN;
    }
}
