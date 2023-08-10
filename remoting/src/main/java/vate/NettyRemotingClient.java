package vate;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.rocketmq.logging.inner.Logger;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyDecoder;
import org.apache.rocketmq.remoting.netty.NettyEncoder;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class NettyRemotingClient extends AbstractNettyRemotingClient {

    Logger logger = Logger.getLogger(NettyRemotingClient.class);
    ConcurrentHashMap<String, Channel> channelCache = new ConcurrentHashMap<>();

    ReentrantLock reentrantLock = new ReentrantLock();

    Bootstrap bootstrap;

    EventLoopGroup workerEventLoopGroup;
    EventExecutorGroup defaultExecutorGroup;

    public NettyRemotingClient() {
    }

    @Override
    public RemotingCommand invokeSync(
        String addr,
        RemotingCommand request,
        long timeoutMillis
    ) throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        Channel channel = getOrCreateChannel(addr, timeoutMillis);
        if (channel == null) {
            throw new RemotingConnectException(addr);
        }
        ResponseFuture responseFuture = new ResponseFuture(channel, request.getOpaque(), timeoutMillis, null, null);
        putResponse(request.getOpaque(), responseFuture);
        // 同步发送请求
        channel.writeAndFlush(request).addListener(future -> {
            if (!future.isSuccess()) {
                removeResponse(request.getOpaque());
                responseFuture.setCause(future.cause());
                responseFuture.setSendRequestOK(false);
                responseFuture.putResponse(null);
            } else {
                responseFuture.setSendRequestOK(true);
            }
        });
        // 等待响应结果
        RemotingCommand remotingCommand = responseFuture.waitResponse(timeoutMillis);
        if (remotingCommand == null) {
            if (responseFuture.isSendRequestOK()) {
                throw new RemotingTimeoutException("超时了");
            } else {
                throw new RemotingSendRequestException(addr);
            }
        }
        return remotingCommand;
    }

    private Channel getOrCreateChannel(String addr, long timeoutMillis) throws InterruptedException {
        long deadLine = System.currentTimeMillis() + timeoutMillis;
        if (reentrantLock.tryLock(deadLine - System.currentTimeMillis(), TimeUnit.MILLISECONDS)) {
            Channel channel = channelCache.get(addr);
            if (Objects.isNull(channel)) {
                try {
                    channel = bootstrap
                        .connect(RemotingUtil.string2SocketAddress(addr))
                        .sync()
                        .channel();
                } catch (Exception e) {
                    // 打异常日志
                    logger.error(e);
                    return null;
                }
            }
            return channel;
        }
        return null;
    }

    @Override
    public void invokeAsync(
        String addr,
        RemotingCommand request,
        long timeoutMillis,
        InvokeCallback invokeCallback
    ) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        Channel channel = getOrCreateChannel(addr, timeoutMillis);
        if (channel == null) {
            throw new RemotingConnectException(addr);
        }
        ResponseFuture responseFuture = new ResponseFuture(channel, request.getOpaque(), timeoutMillis, invokeCallback, null);
        putResponse(request.getOpaque(), responseFuture);
        // 同步发送请求
        channel.writeAndFlush(request).addListener(future -> {
            if (!future.isSuccess()) {
                removeResponse(request.getOpaque());
                responseFuture.setCause(future.cause());
                responseFuture.setSendRequestOK(false);
                responseFuture.putResponse(null);
            } else {
                responseFuture.setSendRequestOK(true);
            }
        });
    }

    @Override
    public boolean isChannelWritable(String addr) {
        return Optional.ofNullable(addr)
            .map(channelCache::get)
            .map(channel -> channel.isActive() && channel.isWritable())
            .orElse(false);
    }

    public void init() {
        defaultExecutorGroup = new DefaultEventExecutorGroup(Runtime.getRuntime().availableProcessors());
        workerEventLoopGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());
    }

    @Override
    public void start() {
        init();
        bootstrap = new Bootstrap()
            .group(workerEventLoopGroup)
            .channel(NioSocketChannel.class)
            .handler(
                new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ch
                            .pipeline()
                            .addLast(defaultExecutorGroup, new NettyEncoder())
                            .addLast(defaultExecutorGroup, new NettyDecoder())
                            .addLast(defaultExecutorGroup, new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                    if (msg instanceof RemotingCommand) {
                                        processRequest(ctx, (RemotingCommand) msg);
                                    }
                                }
                            });
                    }
                }
            );
    }

    @Override
    public void shutdown() {
        workerEventLoopGroup.shutdownGracefully();
        defaultExecutorGroup.shutdownGracefully();
    }
}
