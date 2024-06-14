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
package org.apache.rocketmq.proxy.service.route;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.util.concurrent.TimeUnit;


@RunWith(MockitoJUnitRunner.class)
public class TopicRouteTest {

    @Mock
    MQClientAPIFactory mqClientAPIFactory;

    @Test
    public void testGetAllMessageQueueView() {
        LoadingCache<String, String> build = Caffeine.newBuilder()
            .build(new CacheLoader<String, String>() {
                @Override
                public @Nullable String load(String topic) throws Exception {
                    if (!topic.equals("test")) return null;
                    return topic + "aaaa";
                }

                @Override
                public @Nullable String reload(
                    @NonNull String key,
                    @NonNull String oldValue
                ) throws Exception {
                    try {
                        return load(key);
                    } catch (Exception e) {
                        return oldValue;
                    }
                }
            });
    }
}
