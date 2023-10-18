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

package org.apache.rocketmq.common.thread;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * 线程池状态监听器，专门负责监听线程池某一个指标的状态值。
 */
public interface ThreadPoolStatusMonitor {

    String describe();

    /**
     * @param executor the thread pool that be monit
     * @return the status's value by monitor concerned about
     */
    double value(ThreadPoolExecutor executor);

    /**
     * @param executor the thread pool that be monit
     * @param value the status value of some metric
     * @return whether print the jstack log when the executor's some metric is current value
     */
    boolean needPrintJstack(ThreadPoolExecutor executor, double value);
}
