package vate;


import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * 摘要远程客户端
 *
 * @author 曾碹
 */
public abstract class AbstractNettyRemotingClient extends AbstractNettyRemotingService implements RemotingClient {

    private ExecutorService callbackExecutor;

    @Override
    public void setCallbackExecutor(ExecutorService callbackExecutor) {
        if (Objects.isNull(callbackExecutor)) {
            throw new RuntimeException();
        }
        this.callbackExecutor = callbackExecutor;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.callbackExecutor;
    }

}
