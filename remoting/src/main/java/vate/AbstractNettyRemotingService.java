package vate;


import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.logging.inner.Logger;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public abstract class AbstractNettyRemotingService extends AbstractRemotingService {

    Logger logger = Logger.getLogger(AbstractNettyRemotingService.class);

    public void processRequest(ChannelHandlerContext channelHandlerContext, RemotingCommand remotingCommand) {
        // 如果是响应
        if (remotingCommand.isResponseType()) {
            Optional
                .ofNullable(getResponse(remotingCommand.getOpaque()))
                .ifPresent(responseFuture -> {
                    responseFuture.setResponseCommand(remotingCommand);
                    responseFuture.executeInvokeCallback();
                });
        } else {
            // 如果是请求
            Optional
                .ofNullable(getProcessorPair(remotingCommand.getCode()))
                .ifPresent(pair -> pair.getObject2().submit(
                    () -> {
                        try {
                            channelHandlerContext.writeAndFlush(
                                pair
                                    .getObject1()
                                    .processRequest(channelHandlerContext, remotingCommand)
                            );
                        } catch (Exception e) {
                            // 打日志
                            throw new RuntimeException(e);
                        }
                    }
                ));
        }
    }

}
