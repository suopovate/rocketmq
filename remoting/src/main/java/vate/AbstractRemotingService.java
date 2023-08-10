package vate;


import org.apache.rocketmq.remoting.common.Pair;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public abstract class AbstractRemotingService implements RemotingService {

    private ConcurrentHashMap<Integer, Pair<RequestProcessor, ExecutorService>> processorTables = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, ResponseFuture> responseTables = new ConcurrentHashMap<>();

    @Override
    public void registerProcessor(int requestCode, RequestProcessor processor, ExecutorService executor) {
        if (Objects.isNull(executor)) {
            throw new RuntimeException();
        }
        processorTables.put(requestCode, new Pair<>(processor, executor));
    }

    @Override
    public Pair<RequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
        return processorTables.get(requestCode);
    }

    public void putResponse(int opaque, ResponseFuture responseFuture) {
        responseTables.put(opaque, responseFuture);
    }

    public ResponseFuture getResponse(int opaque) {
        return responseTables.get(opaque);
    }

    public ResponseFuture removeResponse(int opaque) {
        return responseTables.remove(opaque);
    }

}
