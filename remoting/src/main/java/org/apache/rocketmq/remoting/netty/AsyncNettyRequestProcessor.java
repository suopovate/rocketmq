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

package org.apache.rocketmq.remoting.netty;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public abstract class AsyncNettyRequestProcessor implements NettyRequestProcessor {

    /**
     * 涉及应该是希望，在一个异步的线程中处理，然后回调给回调对象
     * 但是这里的默认实现，其实就是个同步的...
     * 至于异步的实现，则是交由各个子类实现，这里只是做了个 异步的调用 形式
     *
     * @param ctx
     * @param request
     * @param responseCallback
     * @throws Exception
     */
    public void asyncProcessRequest(ChannelHandlerContext ctx, RemotingCommand request, RemotingResponseCallback responseCallback) throws Exception {
        RemotingCommand response = processRequest(ctx, request);
        responseCallback.callback(response);
    }
}
