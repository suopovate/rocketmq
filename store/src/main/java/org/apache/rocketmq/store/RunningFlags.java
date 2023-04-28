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

public class RunningFlags {

    /**
     * 1
     */
    private static final int NOT_READABLE_BIT = 1;


    /**
     * 10
     */
    private static final int NOT_WRITEABLE_BIT = 1 << 1;

    /**
     * 100
     */
    private static final int WRITE_LOGICS_QUEUE_ERROR_BIT = 1 << 2;

    /**
     * 1000
     */
    private static final int WRITE_INDEX_FILE_ERROR_BIT = 1 << 3;

    /**
     * 10000
     */
    private static final int DISK_FULL_BIT = 1 << 4;

    private volatile int flagBits = 0;

    public RunningFlags() {
    }

    public int getFlagBits() {
        return flagBits;
    }

    public boolean getAndMakeReadable() {
        boolean result = this.isReadable();
        if (!result) {
            // xxxx & ~0001
            // ~0001 = 1110
            // xxx0
            this.flagBits &= ~NOT_READABLE_BIT;
        }
        return result;
    }

    public boolean isReadable() {
        // xxx0 & 0001 == 0000 可读
        if ((this.flagBits & NOT_READABLE_BIT) == 0) {
            return true;
        }

        return false;
    }

    public boolean getAndMakeNotReadable() {
        boolean result = this.isReadable();
        if (result) {
            // xxx0 |= 0001 : xxx1 不可读
            this.flagBits |= NOT_READABLE_BIT;
        }
        return result;
    }

    public boolean getAndMakeWriteable() {
        boolean result = this.isWriteable();
        if (!result) {
            // xx1x & 1101 = xx0x 可写
            this.flagBits &= ~NOT_WRITEABLE_BIT;
        }
        return result;
    }

    public boolean isWriteable() {
        // xxxx & 00010 | 00100 | 10000 | 01000 : xxxx & 11110 = xxxx0
        // 当 xxxx0 = 00000 时 才是可写
        if ((this.flagBits & (NOT_WRITEABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT | DISK_FULL_BIT | WRITE_INDEX_FILE_ERROR_BIT)) == 0) {
            return true;
        }

        return false;
    }

    /**
     * cmq的话，就算disk满了，也让它写
     */
    //for consume queue, just ignore the DISK_FULL_BIT
    public boolean isCQWriteable() {
        if ((this.flagBits & (NOT_WRITEABLE_BIT | WRITE_LOGICS_QUEUE_ERROR_BIT | WRITE_INDEX_FILE_ERROR_BIT)) == 0) {
            return true;
        }

        return false;
    }

    public boolean getAndMakeNotWriteable() {
        boolean result = this.isWriteable();
        if (result) {
            this.flagBits |= NOT_WRITEABLE_BIT;
        }
        return result;
    }

    public void makeLogicsQueueError() {
        this.flagBits |= WRITE_LOGICS_QUEUE_ERROR_BIT;
    }

    public boolean isLogicsQueueError() {
        if ((this.flagBits & WRITE_LOGICS_QUEUE_ERROR_BIT) == WRITE_LOGICS_QUEUE_ERROR_BIT) {
            return true;
        }

        return false;
    }

    public void makeIndexFileError() {
        this.flagBits |= WRITE_INDEX_FILE_ERROR_BIT;
    }

    public boolean isIndexFileError() {
        if ((this.flagBits & WRITE_INDEX_FILE_ERROR_BIT) == WRITE_INDEX_FILE_ERROR_BIT) {
            return true;
        }

        return false;
    }

    /**
     * @return 是否设置标志成功(如果设置过就返回失败)
     */
    public boolean getAndMakeDiskFull() {
        boolean result = !((this.flagBits & DISK_FULL_BIT) == DISK_FULL_BIT);
        this.flagBits |= DISK_FULL_BIT;
        return result;
    }

    public boolean getAndMakeDiskOK() {
        boolean result = !((this.flagBits & DISK_FULL_BIT) == DISK_FULL_BIT);
        this.flagBits &= ~DISK_FULL_BIT;
        return result;
    }
}
