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

package org.apache.rocketmq.proxy.common.utils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class FutureUtils {

    public static <T> CompletableFuture<T> appendNextFuture(CompletableFuture<T> future,
        CompletableFuture<T> nextFuture, ExecutorService executor) {
        future.whenCompleteAsync((t, throwable) -> {
            if (throwable != null) {
                nextFuture.completeExceptionally(throwable);
            } else {
                nextFuture.complete(t);
            }
        }, executor);
        return nextFuture;
    }

    /**
     * 基于当前future，创建一个nextFuture，这个nextFuture的 结果通知 和 结果逻辑处理，都会在executor的线程里进行。
     * 1. 在executor中，会执行future的结果回调，然后将结果 通知给nextFuture
     * 2. 这时候 nextFuture 的 thenApply 等结果处理逻辑，就会在executor中的结果通知线程里执行。(nextFuture.whenCompleteAsync()这种情况当然就不是了。)
     * 3. 所以本方法叫 addExecutor。
     */
    public static <T> CompletableFuture<T> addExecutor(CompletableFuture<T> future, ExecutorService executor) {
        return appendNextFuture(future, new CompletableFuture<>(), executor);
    }

    public static <T> CompletableFuture<T> completeExceptionally(Throwable t) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(t);
        return future;
    }
}
