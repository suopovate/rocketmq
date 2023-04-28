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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * 功能描述：
 * 这个对象呢，其实就是对file的一个包装，简单点看，他就是个文件，复杂点看
 * 它是一个包含了，写缓冲，mmap机制的文件。
 * <p>
 * <pre>
 * 疑问点：
 * 1. 为什么要搞个写缓冲区？既然已经有用了mmap机制映射到了内存，本身写的时候就是在内存写了。
 *      操作系统的mmap机制，虽然映射了文件区域到内存区域，但是由于虚拟内存的懒加载机制，其实文件映射只是个元数据记录而已，等到我们真正访问文件对应区域内存页的时候，才会触发
 *      缺页异常，同时导致读磁盘的IO操作，所以虽然是映射到了内存，但是仍然还是有IO存在的，只是减少了，对于频繁的读写来说，比起原来的IO，减少了IO的次数，但是仍然会有，
 *      那么这里引入一个堆内存的缓存区就可以理解了，搞一个正儿八经的内存缓冲区，数据先写到这个缓冲区，这个过程是没有IO的，所以会很快就能返回，能提升写入的速率(其实也不全是，因为本身虚拟内存就有swap操作...但是不管怎么说，这些区域经过预热以后，就可以作为通用的内存常驻缓存来用了，还是比较快的)
 * 2. commitPosition 和 flushPosition的区别？
 *      这个mappedFile有两种映射方案，一种是纯粹的使用mmap机制去进行写，一种是writeBuffer + fileChannel
 *      commitPosition就是用来记录writeBuffer已经写入到fileChannel的最新位置的
 * 3. 写消息的时候那个position的设置 不理解
 * </pre>
 */
public class MappedFile extends ReferenceResource {

    /**
     * 操作系统,内存中一个页的大小 4k
     */
    public static final int OS_PAGE_SIZE = 1024 * 4;

    /**
     * 日志实例
     */
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 统计: 总共映射了多少虚拟内存
     */
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);
    /**
     * 统计: 总共映射了多少文件
     */
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);


    /**
     * 写到了哪个位置(记录的是可写的第一个位置)，这个位置不保证数据已经全部被落盘
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    /**
     * 已经提交了缓冲区的数据到哪个位置 这个主要是 writeBuffer + fileChannel配合使用的，这个位置不保证数据已经全部被落盘
     */
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    /**
     * 已经刷新磁盘的数据到了哪个位置，这个位置保证数据落盘
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    /**
     * 文件大小
     */
    protected int fileSize;
    /**
     * 文件通道 文件通道是 writeBuffer + fileChannel使用
     */
    protected FileChannel fileChannel;
    private File file;
    /**
     * 这个缓冲区对象的position、limit、mark指针永远不会变，对缓冲区的修改或者读取，会slip一个映射部分缓冲区内容的子对象来进行操作。
     */
    private MappedByteBuffer mappedByteBuffer;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    protected ByteBuffer writeBuffer = null;
    /**
     * 这个吊东西嘞，相当于是一个缓冲区池子，初始化的时候mappedFile对象会从其中"借"一个缓冲区
     * 等到释放mappedFile的时候又会把缓冲区还回去
     * 这个是管理所有申请的 堆外(直接)内存，项目启动初期就会申请好
     */
    protected TransientStorePool transientStorePool = null;
    private String fileName;

    private long fileFromOffset;
    private volatile long storeTimestamp = 0;
    /**
     * 这特么是要跟那个mappedFileQueue结合用的 标识这个文件是这个队列第一个创建的
     */
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize,
                      final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    /**
     * 主要是用来清理那个mappedBuffer的
     */
    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        // 从buffer中找到最底层的buffer，然后调用它的 cleaner 方法，获取一个清理器，再调用清理器的 clean 方法
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    /**
     * 不断往下找 找到最底层的那个 byteBuffer?
     * 跟这个attachment有很大关系
     */
    private static ByteBuffer viewed(ByteBuffer buffer) {
        // 默认是调用 viewedBuffer 如果有 attachment 就调这个，一直调到 方法返回null为止
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public void init(final String fileName, final int fileSize,
                     final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    /**
     * 这里其实就是简单的创建了文件到内存的映射，并将fileChannel和mappedByteBuffer引用存起来
     */
    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        if (!NumberUtils.isDigits(this.file.getName())){
            throw new IllegalArgumentException("非法的文件名称(文件名应为消息位点)");
        }
        // 这个地方有点搞笑,你直接这么玩？很容易抛出parse异常的...不过也能看出来 他这里的文件都是 消息位点来的
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        // 会创建父目录...
        ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            // mmap机制，由操作系统自己寻找一块连续空闲的虚拟内存区域去映射文件
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    /**
     * 这个就是一个很关键的写方法啦，不过这个方法只是决定用什么缓冲区写，然后将这个可以用来写消息的缓冲区暴露给cb，由调用方自己决定如何往缓冲区写消息，
     * 📢：为什么这么设计呢？我觉得是考虑到 mappedFile 想把“写消息”中 消息内容的处理 这一块抽象出来(比如你是单条还是多条都不管)，而不是写死在mappedFile
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;

        int currentPos = this.wrotePosition.get();

        if (currentPos < this.fileSize) {
            // 有直接内存缓存 就使用写缓存，否则就直接使用mmap映射内存
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            // 这个slice其实相当于是克隆...因为特么这个原始的buffer的各个变量永远不会被修改 所以capacity不会变,position每次都是slice()后为0，然后又设置成这个currentPos
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            if (messageExt instanceof MessageExtBrokerInner) {
                // maxBlank就是这个文件剩余的空白可写空间
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            // 更新当前下一个可写的位置
            this.wrotePosition.addAndGet(result.getWroteBytes());
            // 当前存储的最新的消息的存入时间
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    /**
     * 不是写mmap，是通过fileChannel写数据
     */
    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * 📢：flush操作，不管你是基于 缓存区 + fileChannel 的写，还是直接使用mmap的写，都是在这里统一落盘。
     *     commit方法，只是针对缓存写，将缓存刷到fileChannel去
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            // hold是为了防止文件被关闭，如果文件被关闭了，hold就会失败
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    // 其实从这里可以推断出，他们设计的时候
                    // 如果你有提供内存缓冲 那就使用内存缓冲 + fileChannel
                    // 如果你没有提供内存缓冲 那他就会使用mappedBuffer
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 专门用户将数据写入fileChannel用的
     *
     * 该方法 != 落盘
     * 提交数据 对于writeBuffer + fileChannel 方式是将缓冲区数据提交给文件通道
     *
     * 对于我们的mmap机制来说，则什么都不做，并且返回当且已经写到了哪个位置
     *
     * @param commitLeastPages 攒够多少页提交
     * @return 已经提交的位置 or 已经写了的位置(如果未开启缓存写)
     */
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        // 如果我们已经把当前文件的所有数据都给落盘了，就把写缓冲区还回去
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    /**
     * 这个主要是用来刷新writeBuffer缓冲区的 不过还只是把缓冲区写到fileChannel 其实并没完全保证fileChannel数据一定落盘
     * 落盘还是要fileChannel.force()
     */
    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        // 这段代码有问题吧？？？你他妈 两个位置 相减 肯定大于 这个页啊
        if ((writePos - lastCommittedPosition)/OS_PAGE_SIZE > commitLeastPages) {
            try {
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 判断当前是否满足刷回磁盘的条件
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();

        // 如果文件已经写满了...不管你咋样 都能刷回磁盘
        if (this.isFull()) {
            return true;
        }

        // 只有在当前已经写入的页数 - 当前已经刷写的页数 >= 最少刷新页数 时 才刷新
        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    /**
     * @param commitLeastPages 至少要到了多少页才提交
     * @return
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                // 这个buffer的 可处理范围是 byteBuffer 的 pos - limit 之间的数据
                ByteBuffer byteBufferNew = byteBuffer.slice();
                // 把 limit 又设置成 具体的size，就只会读到这部分的数据，相当于 pos - pos + size(不包含)
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 可读范围是 pos(包括) - readPos(不包括) 之间，这个方法返回的数据，不一定已经落盘了，所以会存在系统崩溃，该消息被丢失的风险。
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                // rp = 3 pos = 0 size = 3 - 0 = 3，不包括rp
                int size = readPosition - pos;
                // 我感觉没必要创建个新的buffer对象 by vate
                 ByteBuffer byteBufferNew = byteBuffer.slice();
                // byteBufferNew.limit(size);
                byteBuffer.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBuffer, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * 尝试清除map的buffer，然后删除文件
     */
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                        + this.getFlushedPosition() + ", "
                        + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                    + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        // 用了写缓存，就使用已提交的位置，就是已提交给fileChannel
        // 没用写缓存，就使用已写的位置
        // 这两个位置 都不是完全的 已刷盘，就是说，这部分数据不会脏，但是可能会丢失，比如突然断电
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * 预热一下？
     * 思路：
     * 把整个mappedByteBuffer部分页的每个字节put一下0...
     * 最终效果就是整个文件都被映射到了物理内存...
     * 这里有一个地方很有意思，就是循环条件，不是循环 fileSize,而是 fileSize / OS_PAGE_SIZE 因为置换都是按页来置换的...
     *
     * 这个方法一旦调用...这个文件就会保持在内存里边，所以我估计rmq应该只维持一个当前写入要用的就可以了
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            // 光是打印一下 还有睡一下 防止gc?感觉跟jit也有点关联
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                    this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
                System.currentTimeMillis() - beginTime);

        // todo vate:  2023-01-10 18:26:30
//        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    /**
     * 如果 buffer的limit = cap，position = 0，那这个新的buffer，就几乎跟原buffer一样，都是从0开始读写
     * 如果 buffer的limit = cap，position > 0，那这个新buffer，就是只能从position开始读写，然后能访问到的内容也是 limit - position的范围
     *
     * 但是我看了下，其实大部分他的使用场景，使用duplicate也是可以的...不知道他们为什么用这个方法，这个方法的命名其实还是比较能说明问题的，
     * 分片，就是从原buffer中，把position - limit 分片出来，只有这块区域可以读写。
     */
    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    /**
     * 将内存“锁住”，防止被置换回磁盘
     */
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("start");
        // mmap机制，由操作系统自己寻找一块连续空闲的虚拟内存区域去映射文件
//        this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
//        MappedFile mappedFile = new MappedFile("0", 1024 * 1024 * 1024);
//        mappedFile.warmMappedFile(FlushDiskType.SYNC_FLUSH, 1024);
//        System.out.println("end");

//        File testLock = new File("testLock");
//        new RandomAccessFile(testLock,"rw").getChannel().lock();
//        System.out.println("拿到锁了！");
//        Thread.sleep(10000000);

        ByteBuffer allocate = ByteBuffer.allocate(5);
        allocate.limit(0);
        allocate.put((byte) 1);
//        allocate.put((byte) 1);
        //        allocate.put((byte) 1);
//        allocate.put((byte) 1);
//        allocate.put((byte) 1);
//
////        allocate.flip();
//        allocate.position(0);
//        allocate.get();
//        allocate.get();

    }

}
