package vate;


import io.netty.channel.Channel;
import org.apache.rocketmq.remoting.common.Pair;

import java.util.concurrent.ExecutorService;

/**
 * 摘要远程服务器
 *
 * @author 曾碹
 */
public abstract class AbstractNettyRemotingServer extends AbstractNettyRemotingService implements RemotingServer<Channel> {

    private ExecutorService callbackExecutor;

    private Pair<RequestProcessor, ExecutorService> defaultProcessor;

    @Override
    public void registerDefaultProcessor(RequestProcessor processor, ExecutorService executor) {
        defaultProcessor = new Pair<>(processor, executor);
    }

}
