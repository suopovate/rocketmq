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
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.index.IndexService;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

/**
 * 消息存储核心类
 * 这里有个乌龙，就是这里的asyncPutMessage和putMessage的逻辑是一模一样的，
 * 甚至都是特么同步的...只是返回的结果有无Completable包装的区别，就算是Completable包装的异步请求，也在进入到cml去写消息的时候是同步加锁写完，
 * 然后用completable包装一下结果。
 *
 * 📢：对于Completable的一个用法做一下解释
 * <pre>
 *     Completable.ofComplete("1").applyAsync(()->xxx1,executor).accept(()->xxx2);
 *     是这样的，第一个ofComplete创建了一个 状态为已完成的 completableStage，所以这时候你调用applyAsync这个方法，
 *     是会立即执行里面的 ()->xxx1 逻辑的，但是由于我们这里是asyncAsync，所以就是会执行 executor.submit(()->xxx1);
 *     (ps. 如果 我们第二个 不是 applyAsync() 是 apply()，那就相当于会立即执行 ()-> xxx1)
 *     然后执行第三个accept(()->xxx)，这时候，假设 applyAsync submit的那个任务 ()->xxx1 比较久还没执行完，
 *     那我们现在调用accept方法的时候，我们上一个阶段的状态就是未完成对吧？那就会把这个 ()->xxx2 作为回调，注册给 上一阶段，
 *     等到上一阶段的 ()->xxx1 执行完以后，上一阶段就会在 ()->xxx1 执行的那个线程，执行我们的 ()->xxx2
 *     📢：是在 上一阶段就会在 ()->xxx1 执行的那个线程 执行我们的 ()->xxx2
 *     📢：假如 我们从 第一个 ofComplete 开始 直到最后的 accept，不管中间多少个步骤，我们都是 同步执行，那其实这整个链条...就跟我们正常的同步调用无异！
 * </pre>
 */
public class DefaultMessageStore implements MessageStore {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final MessageStoreConfig messageStoreConfig;
    // CommitLog
    private final CommitLog commitLog;

    private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;

    private final FlushConsumeQueueService flushConsumeQueueService;

    private final CleanCommitLogService cleanCommitLogService;

    private final CleanConsumeQueueService cleanConsumeQueueService;

    private final IndexService indexService;

    /**
     * 负责创建数据文件
     */
    private final AllocateMappedFileService allocateMappedFileService;

    /**
     * 基于cml，异步构建消费队列
     */
    private final ReputMessageService reputMessageService;

    private final HAService haService;

    /**
     * 消息延迟投递服务
     */
    private final ScheduleMessageService scheduleMessageService;

    /**
     * 统计服务
     */
    private final StoreStatsService storeStatsService;

    /**
     * 内存缓存池
     */
    private final TransientStorePool transientStorePool;

    /**
     * 笼统的记录一下运行的状态
     * 分了五个标志位
     */
    private final RunningFlags runningFlags = new RunningFlags();
    /**
     * 系统时间工具类
     */
    private final SystemClock systemClock = new SystemClock();

    private final ScheduledExecutorService scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread"));
    /**
     * 统计数据管理器，保存了各项统计数据
     */
    private final BrokerStatsManager brokerStatsManager;
    /**
     * 监听器，当消息到达bk时触发。
     */
    private final MessageArrivingListener messageArrivingListener;
    private final BrokerConfig brokerConfig;

    private volatile boolean shutdown = true;

    private StoreCheckpoint storeCheckpoint;

    private AtomicLong printTimes = new AtomicLong(0);

    private final LinkedList<CommitLogDispatcher> dispatcherList;

    private RandomAccessFile lockFile;

    private FileLock lock;

    boolean shutDownNormal = false;

    private final ScheduledExecutorService diskCheckScheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("DiskCheckScheduledThread"));

    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
        final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws IOException {
        this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.brokerStatsManager = brokerStatsManager;
        this.allocateMappedFileService = new AllocateMappedFileService(this);
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            this.commitLog = new DLedgerCommitLog(this);
        } else {
            this.commitLog = new CommitLog(this);
        }
        this.consumeQueueTable = new ConcurrentHashMap<>(32);

        this.flushConsumeQueueService = new FlushConsumeQueueService();
        this.cleanCommitLogService = new CleanCommitLogService();
        this.cleanConsumeQueueService = new CleanConsumeQueueService();
        this.storeStatsService = new StoreStatsService();
        this.indexService = new IndexService(this);
        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.haService = new HAService(this);
        } else {
            this.haService = null;
        }
        this.reputMessageService = new ReputMessageService();

        this.scheduleMessageService = new ScheduleMessageService(this);

        this.transientStorePool = new TransientStorePool(messageStoreConfig);

        if (messageStoreConfig.isTransientStorePoolEnable()) {
            this.transientStorePool.init();
        }

        this.allocateMappedFileService.start();

        this.indexService.start();

        this.dispatcherList = new LinkedList<>();
        this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
        this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());

        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        MappedFile.ensureDirOK(file.getParent());
        lockFile = new RandomAccessFile(file, "rw");
    }

    public void truncateDirtyLogicFiles(long phyOffset) {
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

        for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.truncateDirtyLogicFiles(phyOffset);
            }
        }
    }

    /**
     * @throws IOException
     */
    public boolean load() {
        boolean result = true;

        try {
            // 检查上次退出后，存储目录下，abort文件是否存在，存在就说明上次是意外退出
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown {}", lastExitOK ? "normally" : "abnormally");

            // 定时消息投递服务，实现定时发送消息用的
            if (null != scheduleMessageService) {
                result = result && this.scheduleMessageService.load();
            }

            // 加载commitLog和consumeQueue，都是把mf文件加载到mfq里边，有一个点，这时候是不可写的，因为mf和mfq的offset还没处理
            result = result && this.commitLog.load();
            result = result && this.loadConsumeQueue();

            if (result) {
                // 存储检查点的文件加载
                this.storeCheckpoint =
                    new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
                // 索引文件加载
                this.indexService.load(lastExitOK);
                // 恢复啥呢？主要恢复commitLog和consumerQueue对象的一些offset值，在这里会对mf的writePosition进行恢复，这样后面才可正常进行数据的写入和读取
                this.recover(lastExitOK);

                log.info("load over, and the max phy offset = {}", this.getMaxPhyOffset());
            }
        } catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }

        if (!result) {
            this.allocateMappedFileService.shutdown();
        }

        return result;
    }

    /**
     * 1. 加锁，防止同一个数据目录启动多个进程
     * 2. 检查各个队列的消息位点，并找出这些队列的消息中，最大的物理日志位点(maxPhysicalPosInLogicQueue)，如果maxPhysicalPosInLogicQueue小于已提交日志的最大位点
     *    则恢复，maxPhysicalPosInLogicQueue - 提交日志最大位点，这部分的消息到逻辑队列中。
     * 3. 等待2恢复完成
     * 4. 启用ha服务、启动定时消息投递服务
     * 5. 启动消息队列刷新服务
     * 6. 启动commitLog
     * 7. 启动存储统计服务
     * 8. 创建存储目录？？？这里我不理解，在load的时候不就干过了？
     * 9.
     * @throws Exception
     */
    public void start() throws Exception {

        lock = lockFile.getChannel().tryLock(0, 1, false);
        if (lock == null || lock.isShared() || !lock.isValid()) {
            throw new RuntimeException("Lock failed,MQ already started");
        }

        lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes()));
        lockFile.getChannel().force(true);
        {
            /**
             * 1. Make sure the fast-forward messages to be truncated during the recovering according to the max physical offset of the commitlog;
             * 2. DLedger committedPos may be missing, so the maxPhysicalPosInLogicQueue maybe bigger that maxOffset returned by DLedgerCommitLog, just let it go;
             * 3. Calculate the reput offset according to the consume queue;
             * 4. Make sure the fall-behind messages to be dispatched before starting the commitlog, especially when the broker role are automatically changed.
             */
            long maxPhysicalPosInLogicQueue = commitLog.getMinOffset();
            for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
                for (ConsumeQueue logic : maps.values()) {
                    if (logic.getMaxCommitLogOffset() > maxPhysicalPosInLogicQueue) {
                        maxPhysicalPosInLogicQueue = logic.getMaxCommitLogOffset();
                    }
                }
            }
            if (maxPhysicalPosInLogicQueue < 0) {
                maxPhysicalPosInLogicQueue = 0;
            }
            // 所有消息逻辑队列的最大日志物理位点都小于当前已有的最小物理位点，不是正常情况
            if (maxPhysicalPosInLogicQueue < this.commitLog.getMinOffset()) {
                maxPhysicalPosInLogicQueue = this.commitLog.getMinOffset();
                /**
                 * This happens in following conditions:
                 * 1. If someone removes all the consumequeue files or the disk get damaged.
                 * 2. Launch a new broker, and copy the commitlog from other brokers.
                 *
                 * All the conditions has the same in common that the maxPhysicalPosInLogicQueue should be 0.
                 * If the maxPhysicalPosInLogicQueue is gt 0, there maybe something wrong.
                 */
                log.warn("[TooSmallCqOffset] maxPhysicalPosInLogicQueue={} clMinOffset={}", maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset());
            }
            log.info("[SetReputOffset] maxPhysicalPosInLogicQueue={} clMinOffset={} clMaxOffset={} clConfirmedOffset={}",
                maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset(), this.commitLog.getMaxOffset(), this.commitLog.getConfirmOffset());
            // 设置一下逻辑队列构建服务的起始位点，即，当前所有的逻辑队列中，最末尾的消息的物理位置中，最大的那个物理位置，从这里开始，剩下的日志，就是待构建逻辑队列的日志
            this.reputMessageService.setReputFromOffset(maxPhysicalPosInLogicQueue);
            this.reputMessageService.start();

            /**
             *  处理完那些待构建的逻辑队列的日志,再启动其他服务
             *  1. Finish dispatching the messages fall behind, then to start other services.
             *  2. DLedger committedPos may be missing, so here just require dispatchBehindBytes <= 0
             */
            while (true) {
                if (dispatchBehindBytes() <= 0) {
                    break;
                }
                Thread.sleep(1000);
                log.info("Try to finish doing reput the messages fall behind during the starting, reputOffset={} maxOffset={} behind={}", this.reputMessageService.getReputFromOffset(), this.getMaxPhyOffset(), this.dispatchBehindBytes());
            }
            // 恢复commitLog对象中的队列offset表
            this.recoverTopicQueueTable();
        }

        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.haService.start();
            this.handleScheduleMessageService(messageStoreConfig.getBrokerRole());
        }

        // 启动逻辑队列刷新服务
        this.flushConsumeQueueService.start();
        // 启动提交日志刷新服务
        this.commitLog.start();
        // 启动统计服务
        this.storeStatsService.start();

        this.createTempFile();
        // 加上一系列的磁盘空间检查、cml日志文件删除等任务
        this.addScheduleTask();
        this.shutdown = false;
    }

    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();
            this.diskCheckScheduledExecutorService.shutdown();
            try {

                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.shutdown();
            }
            if (this.haService != null) {
                this.haService.shutdown();
            }

            this.storeStatsService.shutdown();
            this.indexService.shutdown();
            this.commitLog.shutdown();
            // 关闭cmq、index构建服务
            this.reputMessageService.shutdown();
            this.flushConsumeQueueService.shutdown();
            this.allocateMappedFileService.shutdown();
            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();


            // 如果状态是可写，且cmq、index的构建已经跟上了cml的，那就是正常退出，跟不上就是不正常退出
            // todo vate: 为什么要判断可写呢？不加这个行不行？ 2023-04-28 11:54:12
            if (this.runningFlags.isWriteable() && dispatchBehindBytes() == 0) {
                this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
                shutDownNormal = true;
            } else {
                // 如果是不可写状态 或者 还有一部分cml数据没有构建 index 和 cmq
                log.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
            }
        }

        this.transientStorePool.destroy();

        if (lockFile != null && lock != null) {
            try {
                lock.release();
                lockFile.close();
            } catch (IOException e) {
            }
        }
    }

    public void destroy() {
        this.destroyLogics();
        this.commitLog.destroy();
        this.indexService.destroy();
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
    }

    public void destroyLogics() {
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.destroy();
            }
        }
    }

    private PutMessageStatus checkMessage(MessageExtBrokerInner msg) {
        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }

        if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + msg.getPropertiesString().length());
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }
        return PutMessageStatus.PUT_OK;
    }

    private PutMessageStatus checkMessages(MessageExtBatch messageExtBatch) {
        if (messageExtBatch.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + messageExtBatch.getTopic().length());
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }

        if (messageExtBatch.getBody().length > messageStoreConfig.getMaxMessageSize()) {
            log.warn("PutMessages body length too long " + messageExtBatch.getBody().length);
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }

        return PutMessageStatus.PUT_OK;
    }

    private PutMessageStatus checkStoreStatus() {
        if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return PutMessageStatus.SERVICE_NOT_AVAILABLE;
        }

        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("broke role is slave, so putMessage is forbidden");
            }
            return PutMessageStatus.SERVICE_NOT_AVAILABLE;
        }

        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("the message store is not writable. It may be caused by one of the following reasons: " +
                    "the broker's disk is full, write to logic queue error, write to index file error, etc");
            }
            return PutMessageStatus.SERVICE_NOT_AVAILABLE;
        } else {
            this.printTimes.set(0);
        }

        if (this.isOSPageCacheBusy()) {
            return PutMessageStatus.OS_PAGECACHE_BUSY;
        }
        return PutMessageStatus.PUT_OK;
    }

    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {
        PutMessageStatus checkStoreStatus = this.checkStoreStatus();
        if (checkStoreStatus != PutMessageStatus.PUT_OK) {
            return CompletableFuture.completedFuture(new PutMessageResult(checkStoreStatus, null));
        }

        PutMessageStatus msgCheckStatus = this.checkMessage(msg);
        if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
            return CompletableFuture.completedFuture(new PutMessageResult(msgCheckStatus, null));
        }

        long beginTime = this.getSystemClock().now();
        CompletableFuture<PutMessageResult> putResultFuture = this.commitLog.asyncPutMessage(msg);

        // 在执行完消息插入后，记录一些统计信息
        putResultFuture.thenAccept((result) -> {
            long elapsedTime = this.getSystemClock().now() - beginTime;
            if (elapsedTime > 500) {
                log.warn("putMessage not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, msg.getBody().length);
            }
            this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

            if (null == result || !result.isOk()) {
                this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
            }
        });

        return putResultFuture;
    }

    public CompletableFuture<PutMessageResult> asyncPutMessages(MessageExtBatch messageExtBatch) {
        PutMessageStatus checkStoreStatus = this.checkStoreStatus();
        if (checkStoreStatus != PutMessageStatus.PUT_OK) {
            return CompletableFuture.completedFuture(new PutMessageResult(checkStoreStatus, null));
        }

        PutMessageStatus msgCheckStatus = this.checkMessages(messageExtBatch);
        if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
            return CompletableFuture.completedFuture(new PutMessageResult(msgCheckStatus, null));
        }

        long beginTime = this.getSystemClock().now();
        // 消息存储的主要实现逻辑在这
        CompletableFuture<PutMessageResult> resultFuture = this.commitLog.asyncPutMessages(messageExtBatch);

        // 这里只是做个统计
        resultFuture.thenAccept((result) -> {
            long elapsedTime = this.getSystemClock().now() - beginTime;
            if (elapsedTime > 500) {
                log.warn("not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, messageExtBatch.getBody().length);
            }

            this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

            if (null == result || !result.isOk()) {
                this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
            }
        });

        return resultFuture;
    }

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        PutMessageStatus checkStoreStatus = this.checkStoreStatus();
        if (checkStoreStatus != PutMessageStatus.PUT_OK) {
            return new PutMessageResult(checkStoreStatus, null);
        }

        PutMessageStatus msgCheckStatus = this.checkMessage(msg);
        if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
            return new PutMessageResult(msgCheckStatus, null);
        }

        long beginTime = this.getSystemClock().now();
        PutMessageResult result = this.commitLog.putMessage(msg);
        long elapsedTime = this.getSystemClock().now() - beginTime;
        if (elapsedTime > 500) {
            log.warn("not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, msg.getBody().length);
        }

        this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }

    @Override
    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        PutMessageStatus checkStoreStatus = this.checkStoreStatus();
        if (checkStoreStatus != PutMessageStatus.PUT_OK) {
            return new PutMessageResult(checkStoreStatus, null);
        }

        PutMessageStatus msgCheckStatus = this.checkMessages(messageExtBatch);
        if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
            return new PutMessageResult(msgCheckStatus, null);
        }

        long beginTime = this.getSystemClock().now();
        PutMessageResult result = this.commitLog.putMessages(messageExtBatch);
        long elapsedTime = this.getSystemClock().now() - beginTime;
        if (elapsedTime > 500) {
            log.warn("not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, messageExtBatch.getBody().length);
        }

        this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }

    @Override
    public boolean isOSPageCacheBusy() {
        long begin = this.getCommitLog().getBeginTimeInLock();
        long diff = this.systemClock.now() - begin;

        return diff < 10000000
            && diff > this.messageStoreConfig.getOsPageCacheBusyTimeOutMills();
    }

    @Override
    public long lockTimeMills() {
        return this.commitLog.lockTimeMills();
    }

    public SystemClock getSystemClock() {
        return systemClock;
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }

    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
        final int maxMsgNums,
        final MessageFilter messageFilter) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!this.runningFlags.isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        long beginTime = this.getSystemClock().now();

        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        long nextBeginOffset = offset;
        long minOffset = 0;
        long maxOffset = 0;

        GetMessageResult getResult = new GetMessageResult();

        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = consumeQueue.getMinOffsetInQueue();
            maxOffset = consumeQueue.getMaxOffsetInQueue();

            if (maxOffset == 0) {
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = nextOffsetCorrection(offset, 0);
            } else if (offset < minOffset) {
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);
            } else if (offset == maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                nextBeginOffset = nextOffsetCorrection(offset, offset);
            } else if (offset > maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                if (0 == minOffset) {
                    nextBeginOffset = nextOffsetCorrection(offset, minOffset);
                } else {
                    nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
                }
            } else {
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
                if (bufferConsumeQueue != null) {
                    try {
                        status = GetMessageStatus.NO_MATCHED_MESSAGE;

                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        long maxPhyOffsetPulling = 0;

                        int i = 0;
                        final int maxFilterMessageCount = Math.max(16000, maxMsgNums * ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        for (; i < bufferConsumeQueue.getSize() && i < maxFilterMessageCount; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt();
                            long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();

                            maxPhyOffsetPulling = offsetPy;

                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset)
                                    continue;
                            }

                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);

                            if (this.isTheBatchFull(sizePy, maxMsgNums, getResult.getBufferTotalSize(), getResult.getMessageCount(),
                                isInDisk)) {
                                break;
                            }

                            boolean extRet = false, isTagsCodeLegal = true;
                            if (consumeQueue.isExtAddr(tagsCode)) {
                                extRet = consumeQueue.getExt(tagsCode, cqExtUnit);
                                if (extRet) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    // can't find ext content.Client will filter messages by tag also.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}, topic={}, group={}",
                                        tagsCode, offsetPy, sizePy, topic, group);
                                    isTagsCodeLegal = false;
                                }
                            }

                            if (messageFilter != null
                                && !messageFilter.isMatchedByConsumeQueue(isTagsCodeLegal ? tagsCode : null, extRet ? cqExtUnit : null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }

                                continue;
                            }

                            SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                            if (null == selectResult) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                }

                                nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                                continue;
                            }

                            if (messageFilter != null
                                && !messageFilter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }
                                // release...
                                selectResult.release();
                                continue;
                            }

                            this.storeStatsService.getGetMessageTransferedMsgCount().incrementAndGet();
                            getResult.addMessage(selectResult);
                            status = GetMessageStatus.FOUND;
                            nextPhyFileStartOffset = Long.MIN_VALUE;
                        }

                        if (diskFallRecorded) {
                            long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
                            brokerStatsManager.recordDiskFallBehindSize(group, topic, queueId, fallBehind);
                        }

                        nextBeginOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                        long diff = maxOffsetPy - maxPhyOffsetPulling;
                        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE
                            * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
                        getResult.setSuggestPullingFromSlave(diff > memory);
                    } finally {

                        bufferConsumeQueue.release();
                    }
                } else {
                    status = GetMessageStatus.OFFSET_FOUND_NULL;
                    nextBeginOffset = nextOffsetCorrection(offset, consumeQueue.rollNextFile(offset));
                    log.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
                        + maxOffset + ", but access logic queue failed.");
                }
            }
        } else {
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        }

        if (GetMessageStatus.FOUND == status) {
            this.storeStatsService.getGetMessageTimesTotalFound().incrementAndGet();
        } else {
            this.storeStatsService.getGetMessageTimesTotalMiss().incrementAndGet();
        }
        long elapsedTime = this.getSystemClock().now() - beginTime;
        this.storeStatsService.setGetMessageEntireTimeMax(elapsedTime);

        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;
    }

    public long getMaxOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            long offset = logic.getMaxOffsetInQueue();
            return offset;
        }

        return 0;
    }

    public long getMinOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeQueueOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                    return offsetPy;
                } finally {
                    bufferConsumeQueue.release();
                }
            }
        }

        return 0;
    }

    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getOffsetInQueueByTime(timestamp);
        }

        return 0;
    }

    public MessageExt lookMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return lookMessageByOffset(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return this.commitLog.getMessage(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return this.commitLog.getMessage(commitLogOffset, msgSize);
    }

    public String getRunningDataInfo() {
        return this.storeStatsService.toString();
    }

    private String getStorePathPhysic() {
        String storePathPhysic = "";
        if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog()) {
            storePathPhysic = ((DLedgerCommitLog)DefaultMessageStore.this.getCommitLog()).getdLedgerServer().getdLedgerConfig().getDataStorePath();
        } else {
            storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
        }
        return storePathPhysic;
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();

        {
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(getStorePathPhysic());
            result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(physicRatio));

        }

        {

            String storePathLogics = StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir());
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
            result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
        }

        {
            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.buildRunningStats(result);
            }
        }

        result.put(RunningStats.commitLogMinOffset.name(), String.valueOf(DefaultMessageStore.this.getMinPhyOffset()));
        result.put(RunningStats.commitLogMaxOffset.name(), String.valueOf(DefaultMessageStore.this.getMaxPhyOffset()));

        return result;
    }

    @Override
    public long getMaxPhyOffset() {
        return this.commitLog.getMaxOffset();
    }

    @Override
    public long getMinPhyOffset() {
        return this.commitLog.getMinOffset();
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            long minLogicOffset = logicQueue.getMinLogicOffset();

            SelectMappedBufferResult result = logicQueue.getIndexBuffer(minLogicOffset / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            return getStoreTime(result);
        }

        return -1;
    }

    private long getStoreTime(SelectMappedBufferResult result) {
        if (result != null) {
            try {
                final long phyOffset = result.getByteBuffer().getLong();
                final int size = result.getByteBuffer().getInt();
                long storeTime = this.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                return storeTime;
            } catch (Exception e) {
            } finally {
                result.release();
            }
        }
        return -1;
    }

    @Override
    public long getEarliestMessageTime() {
        final long minPhyOffset = this.getMinPhyOffset();
        final int size = this.messageStoreConfig.getMaxMessageSize() * 2;
        return this.getCommitLog().pickupStoreTimestamp(minPhyOffset, size);
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            SelectMappedBufferResult result = logicQueue.getIndexBuffer(consumeQueueOffset);
            return getStoreTime(result);
        }

        return -1;
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }

    @Override
    public SelectMappedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.commitLog.getData(offset);
    }

    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so appendToPhyQueue is forbidden");
            return false;
        }

        boolean result = this.commitLog.appendData(startOffset, data);
        if (result) {
            this.reputMessageService.wakeup();
        } else {
            log.error("appendToPhyQueue failed " + startOffset + " " + data.length);
        }

        return result;
    }

    @Override
    public void executeDeleteFilesManually() {
        this.cleanCommitLogService.excuteDeleteFilesManualy();
    }

    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        QueryMessageResult queryMessageResult = new QueryMessageResult();

        long lastQueryMsgTime = end;

        for (int i = 0; i < 3; i++) {
            QueryOffsetResult queryOffsetResult = this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
            if (queryOffsetResult.getPhyOffsets().isEmpty()) {
                break;
            }

            Collections.sort(queryOffsetResult.getPhyOffsets());

            queryMessageResult.setIndexLastUpdatePhyoffset(queryOffsetResult.getIndexLastUpdatePhyoffset());
            queryMessageResult.setIndexLastUpdateTimestamp(queryOffsetResult.getIndexLastUpdateTimestamp());

            for (int m = 0; m < queryOffsetResult.getPhyOffsets().size(); m++) {
                long offset = queryOffsetResult.getPhyOffsets().get(m);

                try {

                    boolean match = true;
                    MessageExt msg = this.lookMessageByOffset(offset);
                    if (0 == m) {
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

//                    String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
//                    if (topic.equals(msg.getTopic())) {
//                        for (String k : keyArray) {
//                            if (k.equals(key)) {
//                                match = true;
//                                break;
//                            }
//                        }
//                    }

                    if (match) {
                        SelectMappedBufferResult result = this.commitLog.getData(offset, false);
                        if (result != null) {
                            int size = result.getByteBuffer().getInt(0);
                            result.getByteBuffer().limit(size);
                            result.setSize(size);
                            queryMessageResult.addMessage(result);
                        }
                    } else {
                        log.warn("queryMessage hash duplicate, {} {}", topic, key);
                    }
                } catch (Exception e) {
                    log.error("queryMessage exception", e);
                }
            }

            if (queryMessageResult.getBufferTotalSize() > 0) {
                break;
            }

            if (lastQueryMsgTime < begin) {
                break;
            }
        }

        return queryMessageResult;
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        this.haService.updateMasterAddress(newAddr);
    }

    @Override
    public long slaveFallBehindMuch() {
        return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
    }

    @Override
    public long now() {
        return this.systemClock.now();
    }

    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();

            if (!topics.contains(topic) && !topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)
                    && !topic.equals(TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                for (ConsumeQueue cq : queueTable.values()) {
                    cq.destroy();
                    log.info("cleanUnusedTopic: {} {} ConsumeQueue cleaned",
                        cq.getTopic(),
                        cq.getQueueId()
                    );

                    this.commitLog.removeQueueFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
                }
                it.remove();

                if (this.brokerConfig.isAutoDeleteUnusedStats()) {
                    this.brokerStatsManager.onTopicDeleted(topic);
                }

                log.info("cleanUnusedTopic: {},topic destroyed", topic);
            }
        }

        return 0;
    }

    public void cleanExpiredConsumerQueue() {
        long minCommitLogOffset = this.commitLog.getMinOffset();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            if (!topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                Iterator<Entry<Integer, ConsumeQueue>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Entry<Integer, ConsumeQueue> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",
                            nextQT.getValue().getTopic(),
                            nextQT.getValue().getQueueId(),
                            nextQT.getValue().getMaxCommitLogOffset(),
                            nextQT.getValue().getMinLogicOffset());
                    } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                            "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
                            topic,
                            nextQT.getKey(),
                            minCommitLogOffset,
                            maxCLOffsetInConsumeQueue);

                        DefaultMessageStore.this.commitLog.removeQueueFromTopicQueueTable(nextQT.getValue().getTopic(),
                            nextQT.getValue().getQueueId());

                        nextQT.getValue().destroy();
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }

    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset, long maxOffset,
        SocketAddress storeHost) {
        Map<String, Long> messageIds = new HashMap<String, Long>();
        if (this.shutdown) {
            return messageIds;
        }

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQueue());
            maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQueue());

            if (maxOffset == 0) {
                return messageIds;
            }

            long nextOffset = minOffset;
            while (nextOffset < maxOffset) {
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(nextOffset);
                if (bufferConsumeQueue != null) {
                    try {
                        int i = 0;
                        for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            InetSocketAddress inetSocketAddress = (InetSocketAddress) storeHost;
                            int msgIdLength = (inetSocketAddress.getAddress() instanceof Inet6Address) ? 16 + 4 + 8 : 4 + 4 + 8;
                            final ByteBuffer msgIdMemory = ByteBuffer.allocate(msgIdLength);
                            String msgId =
                                MessageDecoder.createMessageId(msgIdMemory, MessageExt.socketAddress2ByteBuffer(storeHost), offsetPy);
                            messageIds.put(msgId, nextOffset++);
                            if (nextOffset > maxOffset) {
                                return messageIds;
                            }
                        }
                    } finally {

                        bufferConsumeQueue.release();
                    }
                } else {
                    return messageIds;
                }
            }
        }
        return messageIds;
    }

    @Override
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {

        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeOffset);
            if (bufferConsumeQueue != null) {
                try {
                    for (int i = 0; i < bufferConsumeQueue.getSize(); ) {
                        i += ConsumeQueue.CQ_STORE_UNIT_SIZE;
                        long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                        return checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
                    }
                } finally {

                    bufferConsumeQueue.release();
                }
            } else {
                return false;
            }
        }
        return false;
    }

    @Override
    public long dispatchBehindBytes() {
        return this.reputMessageService.behind();
    }

    @Override
    public long flush() {
        return this.commitLog.flush();
    }

    @Override
    public boolean resetWriteOffset(long phyOffset) {
        return this.commitLog.resetOffset(phyOffset);
    }

    @Override
    public long getConfirmOffset() {
        return this.commitLog.getConfirmOffset();
    }

    @Override
    public void setConfirmOffset(long phyOffset) {
        this.commitLog.setConfirmOffset(phyOffset);
    }

    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    /**
     * 这个方法，是会有很多线程并发调用的，包括消息的查询，和我们写新消息到cmq的时候
     *
     * 不过，
     * 1. 写消息的线程只有一个
     * 2. 对于已存在的cmq，在加载的时候，肯定就会创建好 cmq 实例，这里不存在新建的问题
     * 3. 对于不存在的cmq，那么在消息写入，或者出现未写入过的cml的Offset的查询时，可能就会走到这里？但是我估计也不可能，因为topic，queueId
     * 肯定前提是知道了cml的位点才行，所以，这里的创建cmq的情况，应该是没有并发，猜测。
     *
     */
    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
        // todo vate: topic自动新建是在哪里触发的？ 2023-01-15 01:07:11
        // 这里是不是有个问题，就是如果这个topic我们没有创建过，这里就可能拿不到，所以这里会自动新建?
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<Integer, ConsumeQueue>(128);
            ConcurrentMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            // 这里应该是考虑到并发的情况，假如两个线程同时进来这里，那这个oldMap可能就是别的线程put的
            // todo vate: 这个地方对ConcurrentMap的使用值得学习 2023-01-15 01:04:26
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        ConsumeQueue logic = map.get(queueId);
        if (null == logic) {
            // 新建消息队列，这个地方应该就是会创建新的cmq文件的地方了，另外一个地方是启动的时候加载，那个不会创建的
            // 这里的话，如果并发到这里，几个线程都创建了一个新的 cmq，好像问题也不大，因为开销不大，也没有在这里加载文件啥的
            // 毕竟cmq本身是一个目录来的，他还不是一个具体文件
            ConsumeQueue newLogic = new ConsumeQueue(
                topic,
                queueId,
                StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                this.getMessageStoreConfig().getMappedFileSizeConsumeQueue(),
                this);
            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        }

        return logic;
    }

    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE || this.getMessageStoreConfig().isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }

    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
        return (maxOffsetPy - offsetPy) > memory;
    }

    private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {

        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        if (maxMsgNums <= messageTotal) {
            return true;
        }

        if (isInDisk) {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }

            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk() - 1) {
                return true;
            }
        } else {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory() - 1) {
                return true;
            }
        }

        return false;
    }

    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }

    /**
     * @throws IOException
     */
    private void createTempFile() throws IOException {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        MappedFile.ensureDirOK(file.getParent());
        boolean result = file.createNewFile();
        log.info(fileName + (result ? " create OK" : " already exists"));
    }

    private void addScheduleTask() {

        // 定时清理cml cmq 过期数据文件
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.cleanFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);

        // 定期检查cml cmq的数据文件，是否正常，不正常就打印error
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.checkSelf();
            }
        }, 1, 10, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (DefaultMessageStore.this.getMessageStoreConfig().isDebugLockEnable()) {
                    try {
                        if (DefaultMessageStore.this.commitLog.getBeginTimeInLock() != 0) {
                            long lockTime = System.currentTimeMillis() - DefaultMessageStore.this.commitLog.getBeginTimeInLock();
                            if (lockTime > 1000 && lockTime < 10000000) {

                                String stack = UtilAll.jstack();
                                final String fileName = System.getProperty("user.home") + File.separator + "debug/lock/stack-"
                                    + DefaultMessageStore.this.commitLog.getBeginTimeInLock() + "-" + lockTime;
                                MixAll.string2FileNotSafe(stack, fileName);
                            }
                        }
                    } catch (Exception e) {
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        // this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        // @Override
        // public void run() {
        // DefaultMessageStore.this.cleanExpiredConsumerQueue();
        // }
        // }, 1, 1, TimeUnit.HOURS);
        // 定时检测磁盘空间，但是不会做删除操作，会打印日志并且设置RunningFlags状态，如果状态为diskFull，会导致消息写入失败，调用方可以监控日志做告警
        // 假如磁盘不够用了，通过磁盘扩容，下一次这里就会发现有可用空间，又会设置diskFull为false了
        this.diskCheckScheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                DefaultMessageStore.this.cleanCommitLogService.isSpaceFull();
            }
        }, 1000L, 10000L, TimeUnit.MILLISECONDS);
    }

    private void cleanFilesPeriodically() {
        this.cleanCommitLogService.run();
        this.cleanConsumeQueueService.run();
    }

    private void checkSelf() {
        this.commitLog.checkSelf();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            Iterator<Entry<Integer, ConsumeQueue>> itNext = next.getValue().entrySet().iterator();
            while (itNext.hasNext()) {
                Entry<Integer, ConsumeQueue> cq = itNext.next();
                cq.getValue().checkSelf();
            }
        }
    }

    private boolean isTempFileExist() {
        // abort文件
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        return file.exists();
    }

    private boolean loadConsumeQueue() {
        // 存储目录
        File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));
        // 每个topic一个目录
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {

            for (File fileTopic : fileTopicList) {
                String topic = fileTopic.getName();

                // 每个队列一个目录
                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId;
                        try {
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        // 队列Id作为队列数据的目录id
                        ConsumeQueue logic = new ConsumeQueue(
                            topic,
                            queueId,
                            StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                            this.getMessageStoreConfig().getMappedFileSizeConsumeQueue(),
                            this);
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!logic.load()) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }

    /**
     * 1. 恢复cmq，找到所有cmq中最末尾的cml位置，且只考虑了，cmq落后于cml的情况，假如cmq的位点大于cml的情况，没有考虑
     * 2. 恢复cml，主要是根据maxPhyOffsetOfConsumeQueue来做一个
     */
    private void recover(final boolean lastExitOK) {
        // 计算逻辑消息队列相关元信息，并返回当前所有消息队列写到的最大的位置
        // 我估计 如果这个值 小于现在 提交日志的最大值 应该意味着数据有缺失，应该是利用commitLog去恢复
        long maxPhyOffsetOfConsumeQueue = this.recoverConsumeQueue();

        if (lastExitOK) {
            // 检查cml中消息内容的完整性，加设置clog的mf的写位点
            this.commitLog.recoverNormally(maxPhyOffsetOfConsumeQueue);
        } else {
            // todo vate: 非正常关闭的恢复逻辑 2023-01-13 17:35:39
            this.commitLog.recoverAbnormally(maxPhyOffsetOfConsumeQueue);
        }

        this.recoverTopicQueueTable();
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public TransientStorePool getTransientStorePool() {
        return transientStorePool;
    }

    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        ConcurrentMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    private long recoverConsumeQueue() {
        // 所有队列的 "最尾消息提交位点值" ，取位点最大的那个 返回
        long latestMsgCommitLogOffset = -1;
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue queue : maps.values()) {
                // 单个队列的恢复，处理完，这个队列就可以写数据了，mf的位点被设置好了，这也是他的主要作用
                // 1. 删除脏数据
                // 2. 找到那个最后可写的位点
                queue.recover();
                if (queue.getMaxCommitLogOffset() > latestMsgCommitLogOffset) {
                    latestMsgCommitLogOffset = queue.getMaxCommitLogOffset();
                }
            }
        }

        return latestMsgCommitLogOffset;
    }

    /**
     * 在cml中存储了当前bk上所有的topic-队列 的 下一个可写位置，在这里利用 consumeQueueTable(在恢复consumer队列的时候已经处理完) 的数据来恢复
     */
    public void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);
        // cml的最小位点
        long minPhyOffset = this.commitLog.getMinOffset();
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();
                // 记录 队列当前写入的最新条目位点
                table.put(key, logic.getMaxOffsetInQueue());
                // 为啥在这更新...
                logic.correctMinOffset(minPhyOffset);
            }
        }

        this.commitLog.setTopicQueueTable(table);
    }

    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    public RunningFlags getAccessRights() {
        return runningFlags;
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> getConsumeQueueTable() {
        return consumeQueueTable;
    }

    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }

    public HAService getHaService() {
        return haService;
    }

    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    public RunningFlags getRunningFlags() {
        return runningFlags;
    }

    public void doDispatch(DispatchRequest req) {
        for (CommitLogDispatcher dispatcher : this.dispatcherList) {
            dispatcher.dispatch(req);
        }
    }

    public void putMessagePositionInfo(DispatchRequest dispatchRequest) {
        ConsumeQueue cq = this.findConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
        cq.putMessagePositionInfoWrapper(dispatchRequest);
    }

    @Override
    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    @Override
    public void handleScheduleMessageService(final BrokerRole brokerRole) {
        if (this.scheduleMessageService != null) {
            if (brokerRole == BrokerRole.SLAVE) {
                this.scheduleMessageService.shutdown();
            } else {
                this.scheduleMessageService.start();
            }
        }

    }

    public int remainTransientStoreBufferNumbs() {
        return this.transientStorePool.availableBufferNums();
    }

    @Override
    public boolean isTransientStorePoolDeficient() {
        return remainTransientStoreBufferNumbs() == 0;
    }

    @Override
    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return this.dispatcherList;
    }

    @Override
    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (map == null) {
            return null;
        }
        return map.get(queueId);
    }

    public void unlockMappedFile(final MappedFile mappedFile) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                mappedFile.munlock();
            }
        }, 6, TimeUnit.SECONDS);
    }

    /**
     * 搞了个队列来处理所有的consumeQueue的写入
     * 跟我昨天洗澡时设想的思路是一样的
     */
    class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
            final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
            switch (tranType) {
                // 非事务消息，或者消息类型为事务已提交
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    DefaultMessageStore.this.putMessagePositionInfo(request);
                    break;
                // 如果事务未提交，或者事务回滚，那就不处理
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
            }
        }
    }

    class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
            if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
                DefaultMessageStore.this.indexService.buildIndex(request);
            }
        }
    }

    class CleanCommitLogService {

        private final static int MAX_MANUAL_DELETE_FILE_TIMES = 20;
        private final double diskSpaceWarningLevelRatio =
            Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));

        private final double diskSpaceCleanForciblyRatio =
            Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));
        private long lastRedeleteTimestamp = 0;

        private volatile int manualDeleteFileSeveralTimes = 0;

        private volatile boolean cleanImmediately = false;

        public void excuteDeleteFilesManualy() {
            this.manualDeleteFileSeveralTimes = MAX_MANUAL_DELETE_FILE_TIMES;
            DefaultMessageStore.log.info("executeDeleteFilesManually was invoked");
        }

        public void run() {
            try {
                this.deleteExpiredFiles();

                this.redeleteHangedFile();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private void deleteExpiredFiles() {
            int deleteCount = 0;
            long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();
            int deletePhysicFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
            int destroyMapedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();

            boolean timeup = this.isTimeToDelete();
            boolean spacefull = this.isSpaceToDelete();
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;

            if (timeup || spacefull || manualDelete) {

                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;

                boolean cleanAtOnce = DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                log.info("begin to delete before {} hours file. timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",
                    fileReservedTime,
                    timeup,
                    spacefull,
                    manualDeleteFileSeveralTimes,
                    cleanAtOnce);

                fileReservedTime *= 60 * 60 * 1000;

                deleteCount = DefaultMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval,
                    destroyMapedFileIntervalForcibly, cleanAtOnce);
                if (deleteCount > 0) {
                } else if (spacefull) {
                    log.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }

        private void redeleteHangedFile() {
            int interval = DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();
            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                this.lastRedeleteTimestamp = currentTimestamp;
                int destroyMapedFileIntervalForcibly =
                    DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
                if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(destroyMapedFileIntervalForcibly)) {
                }
            }
        }

        public String getServiceName() {
            return CleanCommitLogService.class.getSimpleName();
        }

        private boolean isTimeToDelete() {
            String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            if (UtilAll.isItTimeToDo(when)) {
                DefaultMessageStore.log.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }

        private boolean isSpaceToDelete() {
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;

            cleanImmediately = false;

            {
                double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(getStorePathPhysic());
                if (physicRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("physic disk maybe full soon " + physicRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                } else if (physicRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                } else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("physic disk space OK " + physicRatio + ", so mark disk ok");
                    }
                }

                if (physicRatio < 0 || physicRatio > ratio) {
                    DefaultMessageStore.log.info("physic disk maybe full soon, so reclaim space, " + physicRatio);
                    return true;
                }
            }

            {
                String storePathLogics = StorePathConfigHelper
                    .getStorePathConsumeQueue(DefaultMessageStore.this.getMessageStoreConfig().getStorePathRootDir());
                double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
                if (logicsRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                } else if (logicsRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                } else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                    }
                }

                if (logicsRatio < 0 || logicsRatio > ratio) {
                    DefaultMessageStore.log.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
                    return true;
                }
            }

            return false;
        }

        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes;
        }

        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
        }

        /**
         * 检测磁盘的状态，并设置RunningFlags中diskFull的状态。
         */
        public boolean isSpaceFull() {
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(getStorePathPhysic());
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
            if (physicRatio > ratio) {
                DefaultMessageStore.log.info("physic disk of commitLog used: " + physicRatio);
            }
            if (physicRatio > this.diskSpaceWarningLevelRatio) {
                boolean flagUpdateOk = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                if (flagUpdateOk) {
                    DefaultMessageStore.log.error("physic disk of commitLog maybe full soon, used " + physicRatio + ", so mark disk full");
                }

                return true;
            } else {
                boolean flagUpdateOk = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                // 他原先这里是 !diskOk，应该是写错了
                if (flagUpdateOk) {
                    DefaultMessageStore.log.info("physic disk space of commitLog OK " + physicRatio + ", so mark disk ok");
                }

                return false;
            }
        }
    }

    class CleanConsumeQueueService {
        private long lastPhysicalMinOffset = 0;

        public void run() {
            try {
                this.deleteExpiredFiles();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private void deleteExpiredFiles() {
            int deleteLogicsFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();

            long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

                for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                    for (ConsumeQueue logic : maps.values()) {
                        int deleteCount = logic.deleteExpiredFile(minOffset);

                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                                Thread.sleep(deleteLogicsFilesInterval);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }

                DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }

        public String getServiceName() {
            return CleanConsumeQueueService.class.getSimpleName();
        }
    }

    class FlushConsumeQueueService extends ServiceThread {
        private static final int RETRY_TIMES_OVER = 3;
        private long lastFlushTimestamp = 0;

        private void doFlush(int retryTimes) {
            int flushConsumeQueueLeastPages = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            if (retryTimes == RETRY_TIMES_OVER) {
                flushConsumeQueueLeastPages = 0;
            }

            long logicsMsgTimestamp = 0;

            int flushConsumeQueueThoroughInterval = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
                logicsMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

            for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                for (ConsumeQueue cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        result = cq.flush(flushConsumeQueueLeastPages);
                    }
                }
            }

            if (0 == flushConsumeQueueLeastPages) {
                if (logicsMsgTimestamp > 0) {
                    DefaultMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
                }
                DefaultMessageStore.this.getStoreCheckpoint().flush();
            }
        }

        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    int interval = DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    this.waitForRunning(interval);
                    this.doFlush(1);
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            this.doFlush(RETRY_TIMES_OVER);

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushConsumeQueueService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60;
        }
    }

    /**
     * 这个服务，就是基于cml的情况，异步的构建comsumerQueue和index数据
     */
    class ReputMessageService extends ServiceThread {

        private volatile long reputFromOffset = 0;

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        @Override
        public void shutdown() {
            // 关闭时，给一个机会追上提交日志
            for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }
            // 还是没追上，打个日志吧
            if (this.isCommitLogAvailable()) {
                log.warn("shutdown ReputMessageService, but commitlog have not finish to be dispatched, CL: {} reputFromOffset: {}",
                    DefaultMessageStore.this.commitLog.getMaxOffset(), this.reputFromOffset);
            }

            super.shutdown();
        }

        public long behind() {
            return DefaultMessageStore.this.commitLog.getMaxOffset() - this.reputFromOffset;
        }

        private boolean isCommitLogAvailable() {
            return this.reputFromOffset < DefaultMessageStore.this.commitLog.getMaxOffset();
        }

        /**
         * 这里有一个问题，cml有异步刷盘机制，然后cml的话，对外的可读数据是wrotePosition，而不是flushedPosition，
         * wrotePosition 意味着数据已经写到了 内核的pageCache，但是不一定落盘。
         * 所以可能出现：
         * 1. 这里已经将未落盘的cml写到了cmq、index，然后将cmq、index落盘了(理论上有可能哈，无法控制的)
         * 2. 然后断电，cml未落盘部分数据丢失，这时候cmq、index多了几条无效的cml消息索引，怎么办？
         * 3. 首先断电的情况或者，其他非断电但是不正常的关闭情况({@link DefaultMessageStore#shutdown()}里会判断如果cmq、index未跟上cml是不正常)，会标记abort
         * 4. 这时候启动的时候 就会走cml的 abnormalRecover，剩下的就看里面咋处理了
         */
        private void doReput() {
            if (this.reputFromOffset < DefaultMessageStore.this.commitLog.getMinOffset()) {
                log.warn("The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch behind too much and the commitlog has expired.",
                    this.reputFromOffset, DefaultMessageStore.this.commitLog.getMinOffset());
                this.reputFromOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            }
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {

                if (DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable()
                    && this.reputFromOffset >= DefaultMessageStore.this.getConfirmOffset()) {
                    break;
                }

                // 从cml中找出当前待处理的cml数据范围
                SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                if (result != null) {
                    try {
                        this.reputFromOffset = result.getStartOffset();

                        for (int readSize = 0; readSize < result.getSize() && doNext; ) {
                            // 检查消息，并且返回检查结果，和消息的内容
                            DispatchRequest dispatchRequest =
                                DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);
                            int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();

                            if (dispatchRequest.isSuccess()) {
                                if (size > 0) {
                                    // 把消息委派给各个分派服务去处理：consumeQueue增加消息、Index构建等，这里是同步等待的
                                    DefaultMessageStore.this.doDispatch(dispatchRequest);

                                    // 走到这里，意味着cmq和Index都构建好了，不过仍然不保证落盘，因为这两个刷盘是专门的线程干的
                                    // 消息构建成功，可以消费了。
                                    if (BrokerRole.SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                                            && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()
                                            && DefaultMessageStore.this.messageArrivingListener != null) {
                                        DefaultMessageStore.this.messageArrivingListener.arriving(dispatchRequest.getTopic(),
                                            dispatchRequest.getQueueId(), dispatchRequest.getConsumeQueueOffset() + 1,
                                            dispatchRequest.getTagsCode(), dispatchRequest.getStoreTimestamp(),
                                            dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
                                    }

                                    this.reputFromOffset += size;
                                    readSize += size;
                                    if (DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                                        // 对于从节点，统计一下topic的消息存储次数、消息存储size
                                        DefaultMessageStore.this.storeStatsService
                                            .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).incrementAndGet();
                                        DefaultMessageStore.this.storeStatsService
                                            .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                                            .addAndGet(dispatchRequest.getMsgSize());
                                    }
                                } else if (size == 0) {
                                    this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                    readSize = result.getSize();
                                }
                            } else {

                                if (size > 0) {
                                    log.error("[BUG]read total count not equals msg total size. reputFromOffset={}", reputFromOffset);
                                    this.reputFromOffset += size;
                                } else {
                                    doNext = false;
                                    // If user open the dledger pattern or the broker is master node,
                                    // it will not ignore the exception and fix the reputFromOffset variable
                                    if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog() ||
                                        DefaultMessageStore.this.brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
                                        log.error("[BUG]dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                                            this.reputFromOffset);
                                        this.reputFromOffset += result.getSize() - readSize;
                                    }
                                }
                            }
                        }
                    } finally {
                        result.release();
                    }
                } else {
                    doNext = false;
                }
            }
        }

        @Override
        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 每隔一毫秒运行一次，异步把cml中的消息，写入到csq
                    Thread.sleep(1);
                    this.doReput();
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

    }
}
