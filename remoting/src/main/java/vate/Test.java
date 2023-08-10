package vate;

import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.nio.charset.StandardCharsets;

public class Test {

    public static void main(String[] args) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingTooMuchRequestException {
        NettyRemotingServer nettyRemotingServer = new NettyRemotingServer(10220);
        NettyRemotingClient nettyRemotingClient = new NettyRemotingClient();
        nettyRemotingClient.start();
        nettyRemotingServer.start();

        RemotingCommand requestCommand = RemotingCommand.createRequestCommand(8888, null);
        requestCommand.setBody("测试".getBytes(StandardCharsets.UTF_8));
        nettyRemotingClient.invokeAsync(
            "127.0.0.1:10220",
            requestCommand,
            100000, responseFuture -> {
                System.out.println("服务端返回: " + responseFuture.getResponseCommand().getRemark());
            }
        );
    }
}
