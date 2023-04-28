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
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

public class ConsumeQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public static final int CQ_STORE_UNIT_SIZE = 20;
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;

    private final MappedFileQueue mappedFileQueue;
    private final String topic;
    private final int queueId;
    private final ByteBuffer byteBufferIndex;

    private final String storePath;
    private final int mappedFileSize;
    /**
     * 本队列中，最大的有效的条目，在commitLog中的位置
     */
    private long maxCommitLogOffset = -1;
    private volatile long minLogicOffset = 0;
    private ConsumeQueueExt consumeQueueExt = null;

    public ConsumeQueue(
        final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final DefaultMessageStore defaultMessageStore) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.defaultMessageStore = defaultMessageStore;

        this.topic = topic;
        this.queueId = queueId;

        String queueDir = this.storePath
            + File.separator + topic
            + File.separator + queueId;

        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

        if (defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            this.consumeQueueExt = new ConsumeQueueExt(
                topic,
                queueId,
                StorePathConfigHelper.getStorePathConsumeQueueExt(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir()),
                defaultMessageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                defaultMessageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    /**
     * 📢 恢复逻辑其实就是清除脏数据的逻辑，找到最后的那条正常的数据，然后删除这条数据后面的所有数据，就是恢复。
     *
     * 📢 这里有一个点，就是没有考虑消息在cmq中，但是不在cml中这种情况的检测，不过也是，这种检测很耗时，我怀疑这种情况：
     *    1. 要么不会发生cmq中有，cml中没有的情况
     *    2. 要么就是发生了，但是，在查询消息的时候，对这块做了一下容错处理？
     *
     * 搜索当前队列的目录，找到最后一个有效的可写的mf，并将其可写的位置，设置到mfq中
     * 顺便也找到了当前队列中，最大的提交日志位点 maxCommitLogOffset
     *
     * this.mappedFileQueue.setFlushedWhere(processedOffset);
     * this.mappedFileQueue.setCommittedWhere(processedOffset);
     * // 清理掉后续那些
     * this.mappedFileQueue.truncateDirtyFiles(processedOffset);
     */
    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {

            // 倒数第三个元素开始
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            int mappedFileSizeLogics = this.mappedFileSize;
            MappedFile mappedFile = mappedFiles.get(index);
            // 这个时候 mf的mfbuf应该position为0，limit为文件大小的
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            // 已经处理完成的数据偏移量，这里有个tips: 这个值会在每次开始扫描一个文件前，设置为这个文件的 fromOffset，
            long processedOffset = mappedFile.getFileFromOffset();
            // 在当前文件内的偏移量 mf的fileFromOffset <= currentMfOffset <= mf的fileFromOffset + mappedFileSize
            // 这个，代表当前这个文件，最后一条正常的记录的位置
            long currentMfOffset = 0;
            long maxExtAddr = 1;
            while (true) {
                // 遍历该文件
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    // 这里就是很核心的一句，offset >= 0 且 size > 0，为什么是 = 0?因为提交日志位点是从0开始，为什么判断size?因为消息size必须大于0
                    // 为什么要这样判断呢？因为默认情况下整个mf文件，除了写过的位置，其他部分都被初始化为0
                    // size有了，至少保证了offset数据是正常的

                    // 这里有个情况，因为断电的数据缺失，也往往是缺失最后面那一点儿，所以如果当前是正常的，就看下一个条目，
                    // 否则，我们就认为当前不正常，然后清除前一条(currentMfOffset)后面的所有数据
                    if (offset >= 0 && size > 0) {
                        // currentMfOffset 增长，指向下一个条目
                        currentMfOffset = i + CQ_STORE_UNIT_SIZE;
                        // 指向commitLog中的offset，在这里增长
                        this.maxCommitLogOffset = offset + size;
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                            + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }
                // 文件遍历完，则说明该文件的数据都是正常的,如果是最后一个文件，数据没写完，应该是走不到这里来的
                if (currentMfOffset == mappedFileSizeLogics) {
                    // index自增
                    index++;
                    // index = mappedfiles.size，不就意味着 所有文件都写满了，且数据都正常，边界问题，正常退出
                    if (index >= mappedFiles.size()) {
                        log.info("recover last consume queue file over, last mapped file "
                            + mappedFile.getFileName());
                        break;
                    } else {
                        // 走到这里，说明当前文件没问题，扫描下一个文件
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        // 这一句很关键了，每扫描一个文件前，就将 processedOffset 设置为该文件的fromOffset
                        processedOffset = mappedFile.getFileFromOffset();
                        currentMfOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    // 发现某个文件遍历不完，就说明最后一条
                    log.info("recover current consume queue queue over " + mappedFile.getFileName() + " "
                        + (processedOffset + currentMfOffset));
                    break;
                }
            }

            // 走到这里的话呢，processedOffset 肯定是指向最后一个被扫描到有效的文件的fileFromOffset了，
            // currentMfOffset则记录的是当前这个文件最后可用的条目的，下一个条目的位置
            // 所以 下面这条语句得到的 就是 这个文件下一个可写的位置
            processedOffset += currentMfOffset;
            this.mappedFileQueue.setFlushedWhere(processedOffset);
            this.mappedFileQueue.setCommittedWhere(processedOffset);
            // 清理掉后面的脏数据
            this.mappedFileQueue.truncateDirtyFiles(processedOffset);

            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }

    public long getOffsetInQueueByTime(final long timestamp) {
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);
        if (mappedFile != null) {
            long offset = 0;
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            int high = 0;
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long leftIndexValue = -1L, rightIndexValue = -1L;
            long minPhysicOffset = this.defaultMessageStore.getMinPhyOffset();
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                try {
                    while (high >= low) {
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        byteBuffer.position(midOffset);
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }

                        long storeTime =
                            this.defaultMessageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            return 0;
                        } else if (storeTime == timestamp) {
                            targetOffset = midOffset;
                            break;
                        } else if (storeTime > timestamp) {
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        } else {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }

                    if (targetOffset != -1) {

                        offset = targetOffset;
                    } else {
                        if (leftIndexValue == -1) {

                            offset = rightOffset;
                        } else if (rightIndexValue == -1) {

                            offset = leftOffset;
                        } else {
                            offset =
                                Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp
                                    - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }

                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }

    public void truncateDirtyLogicFiles(long phyOffet) {

        int logicFileSize = this.mappedFileSize;

        this.maxCommitLogOffset = phyOffet;
        long maxExtAddr = 1;
        while (true) {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);

                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    if (0 == i) {
                        if (offset >= phyOffet) {
                            this.mappedFileQueue.deleteLastMappedFile();
                            break;
                        } else {
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxCommitLogOffset = offset + size;
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }
                    } else {

                        if (offset >= 0 && size > 0) {

                            if (offset >= phyOffet) {
                                return;
                            }

                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxCommitLogOffset = offset + size;
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }

                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }
            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {

            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    public int deleteExpiredFile(long offset) {
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        this.correctMinOffset(offset);
        return cnt;
    }

    /**
     * 基于cml的最小消息位点，矫正cmq的最小有效位点(针对cmq的项)
     * 因为cml如果没有了这条消息，cmq还保存就没意义了。
     */
    public void correctMinOffset(long phyMinOffset) {
        // cmq 的最小文件
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            // 文件的可读范围
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(0);
            if (result != null) {
                try {
                    // 遍历该文件的所有条目
                    for (int i = 0; i < result.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        // 对应cml的位点
                        long offsetPy = result.getByteBuffer().getLong();
                        result.getByteBuffer().getInt();
                        long tagsCode = result.getByteBuffer().getLong();

                        // 找到 大于 phyMinOffset 的 最早的那个cmq的条目
                        if (offsetPy >= phyMinOffset) {
                            // 相当于是矫正数据了，重新设置 cmq的 最小条目，更早的 其实就没意义了，因为cml中都没有了
                            // 📢 小于 minLogicOffset 的cmq的条目，应该都是不能查的了
                            this.minLogicOffset = mappedFile.getFileFromOffset() + i;
                            log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                                this.getMinOffsetInQueue(), this.topic, this.queueId);
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                minExtAddr = tagsCode;
                            }
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception thrown when correctMinOffset", e);
                } finally {
                    result.release();
                }
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        final int maxRetries = 30;
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            long tagsCode = request.getTagsCode();
            if (isExtWriteEnable()) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());

                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                        topic, queueId, request.getCommitLogOffset());
                }
            }
            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(),
                request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());
            // 更新stockcheckpoint中的cmq存储时间戳
            if (result) {
                if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE ||
                    this.defaultMessageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
                    this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(request.getStoreTimestamp());
                }
                this.defaultMessageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                    + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
    }

    /**
     * 把消息条目存储到cmq中
     * @param offset
     * @param size
     * @param tagsCode
     * @param cqOffset 消息队列条目的位点(非字节)
     * @return
     */
    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,
        final long cqOffset) {
        // todo vate: 待看 2023-01-15 01:29:41
        // 防止重复构建，如果要构建的项的cml位点小于本cmq中记录的最大cml位点，就不再构建
        if (offset + size <= this.maxCommitLogOffset) {
            log.warn("Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}",
                     maxCommitLogOffset, offset);
            return true;
        }

        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);

        // 下一个要写的cmq的字节位点
        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {

            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                this.minLogicOffset = expectLogicOffset;
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                    + mappedFile.getWrotePosition());
            }

            if (cqOffset != 0) {
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

                // 要写入的cmq项小于当前最大的已写入cmq项的位置，判断是重复写入，忽略
                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }

                // 要写入的项的位置，大于当前的最大项了，说明cmq的数据出现问题了，拒绝写入，error告警
                // 这种情况，应该是不会出现，除非人为破坏
                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn(
                        "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset,
                        currentLogicOffset,
                        this.topic,
                        this.queueId,
                        expectLogicOffset - currentLogicOffset
                    );
                }
            }
            this.maxCommitLogOffset = offset + size;
            // 写入文件，不保证刷盘
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }
        return false;
    }

    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }

    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        if (offset >= this.getMinLogicOffset()) {
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
                return result;
            }
        }
        return null;
    }

    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    public long rollNextFile(final long index) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        return index + totalUnitsInFile - index % totalUnitsInFile;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getMaxCommitLogOffset() {
        return maxCommitLogOffset;
    }

    public void setMaxCommitLogOffset(long maxCommitLogOffset) {
        this.maxCommitLogOffset = maxCommitLogOffset;
    }

    public void destroy() {
        this.maxCommitLogOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
            && this.defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }
}
