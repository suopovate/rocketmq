package vate;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyDecoder;
import org.apache.rocketmq.remoting.netty.NettyEncoder;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public class NettyRemotingServer extends AbstractNettyRemotingServer implements RemotingServer<Channel> {

    EventLoopGroup bossEventLoopGroup;
    EventLoopGroup workerEventLoopGroup;
    EventExecutorGroup defaultExecutorGroup;

    ServerBootstrap serverBootstrap;

    Integer port;

    public NettyRemotingServer(Integer port) {
        this.port = port;
    }

    @Override
    public int localListenPort() {
        return port;
    }

    @Override
    public RemotingCommand invokeSync(
        Channel channel,
        RemotingCommand request,
        long timeoutMillis
    ) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
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
                throw new RemotingSendRequestException(RemotingUtil.socketAddress2String(channel.remoteAddress()));
            }
        }
        return remotingCommand;
    }

    @Override
    public void invokeAsync(
        Channel channel,
        RemotingCommand request,
        long timeoutMillis,
        InvokeCallback invokeCallback
    ) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
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

    public void init() {
        defaultExecutorGroup = new DefaultEventExecutorGroup(Runtime.getRuntime().availableProcessors());
        bossEventLoopGroup = new NioEventLoopGroup(1);
        workerEventLoopGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());
        registerProcessor(8888, new EchoRequestProcessor(), Executors.newSingleThreadExecutor());
    }

    @Override
    public void start() {
        init();
        serverBootstrap = new ServerBootstrap();
        serverBootstrap
            .group(bossEventLoopGroup, workerEventLoopGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(
                new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ch.pipeline().addLast(
                            new NettyEncoder(),
                            new NettyDecoder(),
                            new ChannelInboundHandlerAdapter() {
                                @Override
                                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                                    if (msg instanceof RemotingCommand) {
                                        processRequest(ctx, (RemotingCommand) msg);
                                    }
                                }
                            }
                        );
                    }
                }
            );
        try {
            serverBootstrap.bind(port).sync();
        } catch (InterruptedException e) {
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException");
        }
    }

    @Override
    public void shutdown() {
        workerEventLoopGroup.shutdownGracefully();
        bossEventLoopGroup.shutdownGracefully();
        defaultExecutorGroup.shutdownGracefully();
    }

}
