package vate;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

import javax.swing.text.html.Option;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

public class EchoRequestProcessor implements RequestProcessor {

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        RemotingCommand responseCommand = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, null);
        responseCommand.setOpaque(request.getOpaque());
        byte[] body = request.getBody();
        if (Objects.nonNull(body)) {
            String data = new String(body, StandardCharsets.UTF_8);
            System.out.println("收到请求消息：" + data);
            responseCommand.setRemark(data);
        }
        return responseCommand;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

}
