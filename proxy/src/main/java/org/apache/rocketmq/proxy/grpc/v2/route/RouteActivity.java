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
package org.apache.rocketmq.proxy.grpc.v2.route;

import apache.rocketmq.v2.Address;
import apache.rocketmq.v2.AddressScheme;
import apache.rocketmq.v2.Assignment;
import apache.rocketmq.v2.Broker;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Endpoints;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.MessageType;
import apache.rocketmq.v2.Permission;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.Resource;
import com.google.common.net.HostAndPort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.ConfigurationManager;
import org.apache.rocketmq.proxy.grpc.v2.AbstractMessingActivity;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.grpc.v2.common.ResponseBuilder;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.service.route.ProxyTopicRouteData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;

/**
 * <pre>
 *     客户端：
 * 1. 定时从proxy拉取topic的路由信息，调接口时，会在请求头携带proxy地址列表。
 *     mq - broker - address
 * 2. 生产消息时，获取一个mq，通过mq获取address，发送消息，但是这个mq不是最终投递地址，只是用于定位任意一个broker而已。
 * 3. 消费消息时，
 *
 * 服务端：
 * 1. 从ns定时同步topic路由信息，定位到真实broker
 * 2. 客户端来拉取proxyTopicRoute信息时，需要将broker地址改成proxy，再返回路由信息给客户端。
 *
 * 如果是集群模式，所有broker地址，替换为客户端提交的proxy地址列表。
 * 如果是local模式，所有broker地址，替换为其对应的proxy的地址。
 * </pre>
 */
public class RouteActivity extends AbstractMessingActivity {

    public RouteActivity(
        MessagingProcessor messagingProcessor,
        GrpcClientSettingsManager grpcClientSettingsManager, GrpcChannelManager grpcChannelManager
    ) {
        super(messagingProcessor, grpcClientSettingsManager, grpcChannelManager);
    }

    /**
     * 获取某个topic的所有集群路由信息：
     * 即所有的队列信息，每个mq包括：
     * 所在broker的地址信息: brokerName(一个队列实例只在一个broker上) - brokerId(一个broker可以有多个主从实例) - Endpoints(访问这个broker的地址)
     * 每个mq的id、读写权限、消息类型
     * <p>
     * attention: 所有本方法返回的broker的endpoints，都是同一个值，即当前proxy的地址，如果是集群部署，则是整个集群的proxy地址列表
     */
    public CompletableFuture<QueryRouteResponse> queryRoute(ProxyContext ctx, QueryRouteRequest request) {
        CompletableFuture<QueryRouteResponse> future = new CompletableFuture<>();
        try {
            validateTopic(request.getTopic());
            // 从请求中把其请求的端点列表转换为通用的地址列表
            // 这里有一个很特殊的地方，客户端需要传递 其 关注的proxy的地址，也就是这里。
            List<org.apache.rocketmq.proxy.common.Address> addressList = this.convertToAddressList(request.getEndpoints());

            String topicName = request.getTopic().getName();
            // 这里返回的所有broker的地址，都是 addressList
            // 如果是local部署，就是每个broker的地址，替换端口为proxy的端口
            // 如果是cluster，所有broker地址都替换为addressList，即客户端提交的proxy地址列表
            ProxyTopicRouteData proxyTopicRouteData = this.messagingProcessor.getTopicRouteDataForProxy(
                ctx,
                addressList,
                topicName
            );

            List<MessageQueue> messageQueueList = new ArrayList<>();
            // 转换一下broker为grpc对象
            Map<String, Map<Long, Broker>> brokerMap = buildBrokerProtocoMap(proxyTopicRouteData.getBrokerDatas());

            TopicMessageType topicMessageType = messagingProcessor.getMetadataService().getTopicMessageType(ctx, topicName);
            for (QueueData queueData : proxyTopicRouteData.getQueueDatas()) {
                String brokerName = queueData.getBrokerName();
                Map<Long, Broker> brokerIdMap = brokerMap.get(brokerName);
                if (brokerIdMap == null) {
                    break;
                }
                // 把所有的broker的队列信息都提取出来转换为queue，然后放到messageQueueList中
                for (Broker broker : brokerIdMap.values()) {
                    // 每个broker的队列数应该是一样的，但是可读写的队列数不同
                    // 所以对于 topic1 - b0、b1、bn，假设三个队列，最后结果如下：
                    // b0-m0 b0-m1 b0-m2
                    // b1-m0 b1-m1 b1-m2
                    // bn-m0 bn-m1 bn-m2
                    messageQueueList.addAll(this.genMessageQueueFromQueueData(
                        queueData,
                        request.getTopic(),
                        topicMessageType,
                        broker
                    ));
                }
            }

            QueryRouteResponse response = QueryRouteResponse.newBuilder()
                .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                .addAllMessageQueues(messageQueueList)
                .build();
            future.complete(response);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    public CompletableFuture<QueryAssignmentResponse> queryAssignment(ProxyContext ctx, QueryAssignmentRequest request) {
        CompletableFuture<QueryAssignmentResponse> future = new CompletableFuture<>();

        try {
            validateTopicAndConsumerGroup(request.getTopic(), request.getGroup());
            List<org.apache.rocketmq.proxy.common.Address> addressList = this.convertToAddressList(request.getEndpoints());

            ProxyTopicRouteData proxyTopicRouteData = this.messagingProcessor.getTopicRouteDataForProxy(
                ctx,
                addressList,
                request.getTopic().getName()
            );

            boolean fifo = false;
            SubscriptionGroupConfig config = this.messagingProcessor.getSubscriptionGroupConfig(
                ctx,
                request.getGroup().getName()
            );
            if (config != null && config.isConsumeMessageOrderly()) {
                fifo = true;
            }

            List<Assignment> assignments = new ArrayList<>();
            Map<String, Map<Long, Broker>> brokerMap = buildBrokerProtocoMap(proxyTopicRouteData.getBrokerDatas());
            for (QueueData queueData : proxyTopicRouteData.getQueueDatas()) {
                if (PermName.isReadable(queueData.getPerm()) && queueData.getReadQueueNums() > 0) {
                    Map<Long, Broker> brokerIdMap = brokerMap.get(queueData.getBrokerName());
                    if (brokerIdMap != null) {
                        Broker broker = brokerIdMap.get(MixAll.MASTER_ID);
                        Permission permission = this.convertToPermission(queueData.getPerm());
                        if (fifo) {
                            for (int i = 0; i < queueData.getReadQueueNums(); i++) {
                                MessageQueue defaultMessageQueue = MessageQueue.newBuilder()
                                    .setTopic(request.getTopic())
                                    .setId(i)
                                    .setPermission(permission)
                                    .setBroker(broker)
                                    .build();
                                assignments.add(Assignment.newBuilder()
                                                    .setMessageQueue(defaultMessageQueue)
                                                    .build());
                            }
                        } else {
                            MessageQueue defaultMessageQueue = MessageQueue.newBuilder()
                                .setTopic(request.getTopic())
                                .setId(-1)
                                .setPermission(permission)
                                .setBroker(broker)
                                .build();
                            assignments.add(Assignment.newBuilder()
                                                .setMessageQueue(defaultMessageQueue)
                                                .build());
                        }

                    }
                }
            }

            QueryAssignmentResponse response;
            if (assignments.isEmpty()) {
                response = QueryAssignmentResponse.newBuilder()
                    .setStatus(ResponseBuilder.getInstance().buildStatus(Code.FORBIDDEN, "no readable queue"))
                    .build();
            } else {
                response = QueryAssignmentResponse.newBuilder()
                    .addAllAssignments(assignments)
                    .setStatus(ResponseBuilder.getInstance().buildStatus(Code.OK, Code.OK.name()))
                    .build();
            }
            future.complete(response);
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    protected Permission convertToPermission(int perm) {
        boolean isReadable = PermName.isReadable(perm);
        boolean isWriteable = PermName.isWriteable(perm);
        if (isReadable && isWriteable) {
            return Permission.READ_WRITE;
        }
        if (isReadable) {
            return Permission.READ;
        }
        if (isWriteable) {
            return Permission.WRITE;
        }
        return Permission.NONE;
    }

    protected List<org.apache.rocketmq.proxy.common.Address> convertToAddressList(Endpoints endpoints) {

        boolean useEndpointPort = ConfigurationManager.getProxyConfig().isUseEndpointPortFromRequest();

        List<org.apache.rocketmq.proxy.common.Address> addressList = new ArrayList<>();
        for (Address address : endpoints.getAddressesList()) {
            int port = ConfigurationManager.getProxyConfig().getGrpcServerPort();
            if (useEndpointPort) {
                port = address.getPort();
            }
            addressList.add(new org.apache.rocketmq.proxy.common.Address(
                org.apache.rocketmq.proxy.common.Address.AddressScheme.valueOf(endpoints.getScheme().name()),
                HostAndPort.fromParts(address.getHost(), port)
            ));
        }

        return addressList;

    }

    protected Map<String /*brokerName*/, Map<Long /*brokerID*/, Broker>> buildBrokerProtocoMap(
        List<ProxyTopicRouteData.ProxyBrokerData> brokerDataList
    ) {
        Map<String, Map<Long, Broker>> brokerMap = new HashMap<>();
        for (ProxyTopicRouteData.ProxyBrokerData brokerData : brokerDataList) {
            Map<Long, Broker> brokerIdMap = new HashMap<>();
            String brokerName = brokerData.getBrokerName();
            for (Map.Entry<Long, List<org.apache.rocketmq.proxy.common.Address>> entry : brokerData.getBrokerAddrs().entrySet()) {
                Long brokerId = entry.getKey();
                List<Address> addressList = new ArrayList<>();
                AddressScheme addressScheme = AddressScheme.IPv4;
                for (org.apache.rocketmq.proxy.common.Address address : entry.getValue()) {
                    addressScheme = AddressScheme.valueOf(address.getAddressScheme().name());
                    addressList.add(Address.newBuilder()
                                        .setHost(address.getHostAndPort().getHost())
                                        .setPort(address.getHostAndPort().getPort())
                                        .build());
                }

                Broker broker = Broker.newBuilder()
                    .setName(brokerName)
                    .setId(Math.toIntExact(brokerId))
                    .setEndpoints(Endpoints.newBuilder()
                                      .setScheme(addressScheme)
                                      .addAllAddresses(addressList)
                                      .build())
                    .build();

                brokerIdMap.put(brokerId, broker);
            }
            brokerMap.put(brokerName, brokerIdMap);
        }
        return brokerMap;
    }

    protected List<MessageQueue> genMessageQueueFromQueueData(
        QueueData queueData, Resource topic,
        TopicMessageType topicMessageType, Broker broker
    ) {
        List<MessageQueue> messageQueueList = new ArrayList<>();

        int r = 0;
        int w = 0;
        int rw = 0;
        // 不知道这套机制 是不是单纯是proxy的实现，还是和broker的实现有关系
        if (PermName.isWriteable(queueData.getPerm()) && PermName.isReadable(queueData.getPerm())) {
            // r 3 w 5
            //final: rw = 3 r = 0 w = 2
            // 0 1 (可写) 2 3 4 (可读可写)
            // 假如 r 5 w 3
            // 0 1 (可读) 2 3 4 (可读可写)
            rw = Math.min(queueData.getWriteQueueNums(), queueData.getReadQueueNums());
            r = queueData.getReadQueueNums() - rw;
            w = queueData.getWriteQueueNums() - rw;
        } else if (PermName.isWriteable(queueData.getPerm())) {
            w = queueData.getWriteQueueNums();
        } else if (PermName.isReadable(queueData.getPerm())) {
            r = queueData.getReadQueueNums();
        }

        // r here means readOnly queue nums, w means writeOnly queue nums, while rw means both readable and writable queue nums.
        int queueIdIndex = 0;
        for (int i = 0; i < r; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(queueIdIndex++)
                .setPermission(Permission.READ)
                .addAllAcceptMessageTypes(parseTopicMessageType(topicMessageType))
                .build();
            messageQueueList.add(messageQueue);
        }

        for (int i = 0; i < w; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(queueIdIndex++)
                .setPermission(Permission.WRITE)
                .addAllAcceptMessageTypes(parseTopicMessageType(topicMessageType))
                .build();
            messageQueueList.add(messageQueue);
        }

        for (int i = 0; i < rw; i++) {
            MessageQueue messageQueue = MessageQueue.newBuilder().setBroker(broker).setTopic(topic)
                .setId(queueIdIndex++)
                .setPermission(Permission.READ_WRITE)
                .addAllAcceptMessageTypes(parseTopicMessageType(topicMessageType))
                .build();
            messageQueueList.add(messageQueue);
        }

        return messageQueueList;
    }

    private List<MessageType> parseTopicMessageType(TopicMessageType topicMessageType) {
        switch (topicMessageType) {
            case NORMAL:
                return Collections.singletonList(MessageType.NORMAL);
            case FIFO:
                return Collections.singletonList(MessageType.FIFO);
            case TRANSACTION:
                return Collections.singletonList(MessageType.TRANSACTION);
            case DELAY:
                return Collections.singletonList(MessageType.DELAY);
            case MIXED:
                return Arrays.asList(MessageType.NORMAL, MessageType.FIFO, MessageType.DELAY, MessageType.TRANSACTION);
            default:
                return Collections.singletonList(MessageType.MESSAGE_TYPE_UNSPECIFIED);
        }
    }
}
