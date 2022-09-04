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

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * K2 这个MappedFileQueue对应CommitLog目录下的文件
 */
public class MappedFileQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private static final int DELETE_FILES_BATCH_MAX = 10;
    private final String storePath; // 存储目录
    private final int mappedFileSize; // 单个文件大小
    // MappedFile文件集合
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();
    private final AllocateMappedFileService allocateMappedFileService; // 创建MapFile服务类
    private long flushedWhere = 0; //当前刷盘指针
    private long committedWhere = 0; // 当前数据提交指针,内存中ByteBuffer当前的写指针,该值大于等于flushWhere
    private volatile long storeTimestamp = 0;

    public MappedFileQueue(final String storePath, int mappedFileSize, AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    public void checkSelf() { // 检查mappedFiles中，除去最后一个文件，其余每一个mappedFile的大小是否是mappedFileSize
        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}", pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    /**
     * 根据时间戳获取MappedFile文件，将MappedFile队列排序，并且遍历，直到某个MappedFile文件的最后修改时间大于指定时间戳，则返回该队列，
     * 如果没有大于指定时间戳的MappedFile则返回最后一个。
     */
    public MappedFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);
        if (null == mfs) return null;
        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }
        return (MappedFile) mfs[mfs.length - 1];
    }

    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;
        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }
        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    public void truncateDirtyFiles(long offset) { // 清除offset之后的脏数据
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();
        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) { // 重置offset所在的MappedFile的position
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else { // 清理offset所在的MappedFile之后的文件
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }

        this.deleteExpiredFile(willRemoveFiles);
    }

    void deleteExpiredFile(List<MappedFile> files) {
        if (!files.isEmpty()) {
            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }
            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    public boolean load() {
        File dir = new File(this.storePath); // 根据配置的storePath路径加载文件
        File[] files = dir.listFiles();
        if (files != null) {
            Arrays.sort(files); // 按顺序加载到队列中
            for (File file : files) {
                // 若文件大小不等于mappedFileSize则退出，一般发生在最后一个，故在RocketMQ第一次启动之后就不能再修改mappedFileSize大小或复制，导致存储的消息错乱无法恢复。
                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length() + " length not matched message store config value, please check it manually");
                    return false;
                }
                try { // 创建MappedFile对应，将本地文件映射到内存中
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);
                    // 修改MappedFile的位置信息
                    mappedFile.setWrotePosition(this.mappedFileSize);       // 已写位置
                    mappedFile.setFlushedPosition(this.mappedFileSize);     // 设置已刷盘位置
                    mappedFile.setCommittedPosition(this.mappedFileSize);   //设置已提交位置
                    this.mappedFiles.add(mappedFile); // 添加到内存文件映射集合中
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }
        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    /**
     * 根据条件从队列里获取最新的一个MappedFile文件，可以根据时间戳获取，也可以根据偏移量获取，或是直接从队列中第一个和最后一个
     *
     * @param startOffset - 开始偏移量
     * @param needCreate  - 是否需要创建
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        MappedFile mappedFileLast = getLastMappedFile(); // 获取MappedFile文件队列中最后一个文件，因为是按照顺序写的
        if (mappedFileLast == null) { // 说明需要创建MappedFile，计算创建文件的起始offset即文件名
            // 若当前MappedFileQueue为空，则要创建的文件的起始offset为不大于startOffset的最大能被mappedFileSize整除的数
            // 如[0,99),[100,199),[200,299)队列梯度这样的，若需要拉取230偏移量所在的队列起始队列，那么就是230 - （230 % 100）= 200
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }
        if (mappedFileLast != null && mappedFileLast.isFull()) { // 若文件满了也需要创建MappedFile文件，计算创建文件的起始offset即文件名
            // 如果最后一个队列满了，则从最后一个队列的起始偏移位置 + 队列长度作为下一个队列的起始偏移位置
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }
        if (createOffset != -1 && needCreate) { // 若创建文件的起始offset不为-1，其需要创建MapperFile文件
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
            MappedFile mappedFile = null;
            if (this.allocateMappedFileService != null) {
                mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath, nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mappedFile exception", e);
                }
            }
            if (mappedFile != null) {
                if (this.mappedFiles.isEmpty()) { // 若MappedFile是第一个被创建的，会加上标识，这个标识在put消息的时候会使用到
                    mappedFile.setFirstCreateInQueue(true);
                }
                this.mappedFiles.add(mappedFile);  // 将创建的MappedFile加入队列中
            }
            return mappedFile;
        }
        return mappedFileLast;
    }

    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    // 获取最后一个文件的映射对象（MappedFile）, 如果最后一个文件已经写满了，重新创建一个新文件
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;
        while (!this.mappedFiles.isEmpty()) {
            try {// 获取MappedFileQueue中的最后一个MappedFile类实例
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }
        return mappedFileLast;
    }

    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();
        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() + mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset; //最后写到的位置与要求的offset之间的差距
            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff) { // 如果超过了2个fileSize就失败
                return false;
            }
        }
        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();
        while (iterator.hasPrevious()) { //这里似乎是bug，总会返回false，因为上一行cursor会是0
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    public long getMinOffset() { // 获取最小的offset，也就是这一系列文件的最小起始位置，第一个文件的名
        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    public long getMaxOffset() { // 实际上就是commit的位置
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    /**
     * 获取最大写的位置，即最后一个MappedFile写到的位置
     */
    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    /**
     * 还有多少字节等待commit的，即wrote与commit位置之差
     */
    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    /**
     * 还有多少字节等待flush的，即flush与commit位置之差
     */
    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    // 删除，清理最后一个mappedFile
    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());
        }
    }

    // 指定过期时间删除MappedFile文件，expiredTime默认72小时，deleteFilesInterval默认100，intervalForcibly默认120s，cleanImmediately默认false
    public int deleteExpiredFileByTime(final long expiredTime, final int deleteFilesInterval, final long intervalForcibly, final boolean cleanImmediately) {
        Object[] mfs = this.copyMappedFiles(0);
        if (null == mfs) return 0;
        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<MappedFile>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) { // 遍历MappedFile队列，满足条件的全部删除
                MappedFile mappedFile = (MappedFile) mfs[i];
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                // 如果MappedFile的最后修改时间过期或者是cleanImmediately立即清除，默认72小时失效，则会执行清除操作
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (mappedFile.destroy(intervalForcibly)) { // 调用MappedFile的destroy方法，释放连接通道，并且加入删除文件队列
                        files.add(mappedFile); // 加入删除文件队列
                        deleteCount++;
                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break; // 一次最多删除10个
                        }
                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try { // 删除了一个文件之后，等待一段时间再删除下一个，默认100ms
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }
        deleteExpiredFile(files); // 从mappedFiles中删除记录
        return deleteCount;
    }

    public int deleteExpiredFileByOffset(long offset, int unitSize) { // 根据消息偏移量删除过期文件
        Object[] mfs = this.copyMappedFiles(0);
        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {
            int mfsLength = mfs.length - 1;
            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                // 获取映射文件最后一个位置的索引，若result为null表明该映射文件还未填充完，即不存在下一个位置索引文件，因此无需删除当前的位置索引文件。
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong(); // 该文件最大的offSet
                    // 调用mappedFile.selectMappedBuffer方法时，持有计数器加1，因此，查询完后，要释放引用，持有计数器减1.
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset; // 如果队列最大偏移量小于需要删除的位点，则需要进行删除
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset " + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }
                // 调用MappedFile的destroy方法，释放连接通道，并且加入删除文件队列
                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile); // 需要删除的文件
                    deleteCount++;
                } else {
                    break;
                }
            }
        }
        deleteExpiredFile(files);
        return deleteCount;
    }

    // 根据当前记录的刷新和提交标志位获取对应的MappedFile，调用MappedFile.flush/commit
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        // 通过刷盘的offset，从mappedFiles列表中找出offset具体所在的MappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp(); // 文件最后一次内容写入时间
            int offset = mappedFile.flush(flushLeastPages); // 刷盘，得到新的刷到的位置，相对该mappedFile的位置
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.flushedWhere; // 判断是否刷盘成功
            this.flushedWhere = where;
            if (0 == flushLeastPages) { // 若没有内容可刷新
                this.storeTimestamp = tmpTimeStamp; // 更新文件最后一次内容写入时间
            }
        }
        return result;
    }

    /**
     * 上一次的committedWhere对应的mappedFile执行commit更新committedWhere，result为false代表真正的commit了
     */
    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        // 通过Commit的offset，从mappedFiles列表中找出offset具体所在的MappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            int offset = mappedFile.commit(commitLeastPages); //执行commit之后，mappedFile记录的相对commit的位置
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.committedWhere; // false才代表commit执行了
            this.committedWhere = where;
        }
        return result;
    }

    /**
     * Finds a mapped file by offset.
     *
     * @param offset                Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MappedFile firstMappedFile = this.getFirstMappedFile();
            MappedFile lastMappedFile = this.getLastMappedFile();
            if (firstMappedFile != null && lastMappedFile != null) {
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}", offset,
                            firstMappedFile.getFileFromOffset(), lastMappedFile.getFileFromOffset() + this.mappedFileSize, this.mappedFileSize, this.mappedFiles.size());
                } else {
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }
                    if (targetFile != null && offset >= targetFile.getFileFromOffset() && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }
                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset() && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }
                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }
        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;
        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }
        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    // 删除第一个文件
    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
