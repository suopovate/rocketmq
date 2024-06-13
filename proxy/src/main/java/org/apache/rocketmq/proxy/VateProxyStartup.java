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

package org.apache.rocketmq.proxy;

/**
 * 代理模式分为cluster和local两种。
 * cluster：等于单独有一个Proxy集群。
 * local：每个proxy进程里同时启动一个正常的broker进程，请求发给proxy，proxy转发给对应的broker。
 * 类设计：
 * ServiceManager - XxxService
 * ClusterServiceManager - XxxClusterService
 * LocalServiceManager - XxxLocalService
 * 具体流程：
 * ServiceManagerFactory ---proxyMode---> 获取 local/cluster ServiceManager -> 获取 local/cluster XxxService -> 调用具体api
 * 如果是cluster，请求进来，转发给对应的broker。
 * 如果是local，请求进来，判断是否为本broker，是则直接调用brokerController对应的processor的接口，否则，走网络请求调用对应的broker接口。
 */
public class VateProxyStartup {
    public static void main(String[] args) {
        System.setProperty("rocketmq.home.dir", "/Users/vate/softcache/idea-rocketMq-5");
        ProxyStartup.main(new String[]{
            "--brokerConfigPath", "/Users/vate/softcache/workspace_java/mines/rocketmq_official/distribution/conf/broker.conf",
            "--namesrvAddr", "127.0.0.1:9876",
            "--proxyConfigPath", "/Users/vate/softcache/workspace_java/mines/rocketmq_official/distribution/conf/rmq-proxy.json",
            "--proxyMode", "local",
            });
    }

}
