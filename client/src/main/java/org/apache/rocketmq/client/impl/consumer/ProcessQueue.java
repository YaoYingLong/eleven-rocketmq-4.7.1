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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;

/**
 * Queue consumption snapshot 一个队列消费快照，消息拉取的时候，会将实际消息体、拉取相关的操作存放在其中
 */
public class ProcessQueue {
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));
    private final InternalLogger log = ClientLogger.getLog();
    private final ReadWriteLock lockTreeMap = new ReentrantReadWriteLock(); // 读写锁：堆笑的操作都要使用
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>(); // 临时存放消息
    private final AtomicLong msgCount = new AtomicLong(); // 消息总数据量
    private final AtomicLong msgSize = new AtomicLong();    // 整个ProcessQueue处理单元的总消息长度
    private final Lock lockConsume = new ReentrantLock();
    /**
     * A subset of msgTreeMap, will only be used when orderly consume
     */
    // 一个临时的TreeMap，仅在顺序消费模式下使用
    private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<Long, MessageExt>();
    private final AtomicLong tryUnlockTimes = new AtomicLong(0);
    private volatile long queueOffsetMax = 0L; // 整个ProcessQueue处理单元的offset最大边界
    private volatile boolean dropped = false; // 是否被删除
    private volatile long lastPullTimestamp = System.currentTimeMillis(); // 最后拉取时间
    private volatile long lastConsumeTimestamp = System.currentTimeMillis(); // 最后消费时间
    private volatile boolean locked = false;
    private volatile long lastLockTimestamp = System.currentTimeMillis();
    private volatile boolean consuming = false; // 是否正在消费
    private volatile long msgAccCnt = 0; // 拉取消息的那一刻broker端还有多少条消息没有被处理

    // 在顺序消费时使用，用来判断向broker请求消息时，对ProcessQueue锁定时间是否超过阈值，没有超时，代表还是持有锁，超过这个时间则失效
    public boolean isLockExpired() { // 在处理消息时，判断拥有的锁是否过期，默认过期时间30
        return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
    }
    // 在负载均衡更新ProcessQueueTable时调用，如果拉取失效，ProcessQueue将被丢弃
    public boolean isPullExpired() { // 判断拉取消息是否过期，默认两分钟120s
        return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
    }

    /**
     * @param pushConsumer
     */
    // 清理过期的消息，最多16条消息一处理，消息在客户端存在超过15分钟就被认为已过期，然后从本地缓存中移除，以10s的延时消息方式发送会Broker
    public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
        if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly()) {
            return; // 顺序消息直接跳过，即顺序消息不清理过期消息
        }
        int loop = msgTreeMap.size() < 16 ? msgTreeMap.size() : 16; // 每次最多处理16条
        for (int i = 0; i < loop; i++) { // 遍历临时存放消息
            MessageExt msg = null;
            try {
                this.lockTreeMap.readLock().lockInterruptibly(); // 处理消息之前，先拿到读锁
                try { // 临时存放消息的treeMap不为空，并且 判断当前时间 - TreeMap里第一条消息的开始消费时间 > 15分钟
                    if (!msgTreeMap.isEmpty() && System.currentTimeMillis() - Long.parseLong(MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue())) > pushConsumer.getConsumeTimeout() * 60 * 1000) {
                        msg = msgTreeMap.firstEntry().getValue();// 把第一条消息拿出来
                    } else {
                        break;
                    }
                } finally {
                    this.lockTreeMap.readLock().unlock();  // 释放读锁
                }
            } catch (InterruptedException e) {
                log.error("getExpiredMsg exception", e);
            }
            try { // 把过期消息以延时消息方式重新发给broker，10s之后才能消费
                pushConsumer.sendMessageBack(msg, 3);
                log.info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                try {
                    this.lockTreeMap.writeLock().lockInterruptibly(); // 获取写锁
                    try {
                        if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
                            try {// 将过期消息从本地缓存中的消息列表中移除掉，Collections.singletonList表示只有一个元素的List集合
                                removeMessage(Collections.singletonList(msg));
                            } catch (Exception e) {
                                log.error("send expired msg exception", e);
                            }
                        }
                    } finally {
                        this.lockTreeMap.writeLock().unlock(); // 释放写锁
                    }
                } catch (InterruptedException e) {
                    log.error("getExpiredMsg exception", e);
                }
            } catch (Exception e) {
                log.error("send expired msg exception", e);
            }
        }
    }

    public boolean putMessage(final List<MessageExt> msgs) {
        boolean dispatchToConsume = false; // 这个只有在顺序消费的时候才会遇到，并发消费不会用到
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                int validMsgCnt = 0;//有效消息数量
                for (MessageExt msg : msgs) { // 把传过来的消息都都放在msgTreeMap中，以消息在queue中的offset作为key，msg做为value
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                    if (null == old) { // 正常情况，说明原本msgTreeMap中不包含此条消息
                        validMsgCnt++;
                        this.queueOffsetMax = msg.getQueueOffset();
                        msgSize.addAndGet(msg.getBody().length);
                    }
                }
                msgCount.addAndGet(validMsgCnt); // 增加有效消息数量
                // msgTreeMap不为空(含有消息)，并且不是正在消费状态，这个值在放消息的时候会设置为true，在顺序消费模式，取不到消息则设置为false
                if (!msgTreeMap.isEmpty() && !this.consuming) {
                    dispatchToConsume = true; // 有消息，且为未消费状态，则顺序消费模式可以消费
                    this.consuming = true; // 将ProcessQueue置为正在被消费状态
                }
                if (!msgs.isEmpty()) {
                    MessageExt messageExt = msgs.get(msgs.size() - 1); // 拿到最后一条消息
                    // 获取broker端（拉取消息时）queue里最大的offset，maxOffset会存在每条消息里
                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                    if (property != null) { // 计算broker端还有多少条消息没有被消费
                        // broker端的最大偏移量 - 当前ProcessQueue中处理的最大消息偏移量
                        long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
                        if (accTotal > 0) {
                            this.msgAccCnt = accTotal;
                        }
                    }
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }
        return dispatchToConsume;
    }

    public long getMaxSpan() { // 返回processQueue中处理的一批消息中最大offset和最小offset之间的差距
        try { // 根据这个判断当前还有多少消息未处理，如果大于某个值(默认2000)，则进行流控处理：将PullRequest先存起来，默认过50ms再执行拉消息逻辑
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }

    // 在并发消费后进行调用，消息处理完之后将ProcessQueue中msgTreeMap红黑树的这批消息移除
    public long removeMessage(final List<MessageExt> msgs) { // 将从ProcessQueue中移除部分消息，并行消费模式中使用
        long result = -1; // 当前消费进度的下一个offset
        final long now = System.currentTimeMillis();
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();// 获取写锁
            this.lastConsumeTimestamp = now;// 最后消费的时间
            try {
                if (!msgTreeMap.isEmpty()) {
                    result = this.queueOffsetMax + 1;
                    int removedCnt = 0; // 删除的消息个数
                    for (MessageExt msg : msgs) { // 遍历消息，将其从TreeMap中移除
                        MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                        if (prev != null) { // 不为null，说明删除成功
                            removedCnt--; // 已经移除的消息数量
                            msgSize.addAndGet(0 - msg.getBody().length); // ProcessQueue中消息的长度 - 当前消息长度
                        }
                    }
                    msgCount.addAndGet(removedCnt); // ProcessQueue中的消息数量 - 删除的消息数量，即加上removedCnt（是一个负数）
                    if (!msgTreeMap.isEmpty()) {
                        result = msgTreeMap.firstKey();
                    }
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (Throwable t) {
            log.error("removeMessage exception", t);
        }
        return result;
    }

    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }

    public AtomicLong getMsgCount() {
        return msgCount;
    }

    public AtomicLong getMsgSize() {
        return msgSize;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    // 在顺序消费客户端处理消息后，如果消息处理结果的状态为ROLLBACK，此时调用ProcessQueue#rollback方法；将msgTreeMapTmp中的消息重新写回msgTreeMap中；ROLLBACK状态在顺序消费中已不建议使用。
    public void rollback() { // 把临时TreeMap中的消息全部放到msgTreeMap中，等待下一次消费
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try { // 把临时TreeMap中的消息全部放到msgTreeMap中，等待下一次消费
                this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
                this.consumingMsgOrderlyTreeMap.clear();
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    // 在顺序消费模式下，调用takeMessages从msgTreeMap中获取消息：其内部会将消息都放在一个临时的TreeMap中，然后进行消费。
    // 消费完消息之后，需要调用commit()方法将这个临时的consumingMsgOrderlyTreeMap清除
    public long commit() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                Long offset = this.consumingMsgOrderlyTreeMap.lastKey(); // 获取临时TreeMap中最后一个消息的offset
                msgCount.addAndGet(0 - this.consumingMsgOrderlyTreeMap.size()); // 消费完成之后，减去该批次的消息数量
                for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {  // 维护ProcessQueue中的总消息长度
                    msgSize.addAndGet(0 - msg.getBody().length); // 减去每条已消费消息的长度
                }
                this.consumingMsgOrderlyTreeMap.clear(); // 清除临时TreeMap中的所有消息
                if (offset != null) {
                    return offset + 1; // 返回下一个消费进度
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("commit exception", e);
        }
        return -1;
    }

    // 当客户端返回消息状态为SUSPEND_CURRENT_QUEUE_A_MOMENT时调用；其将消息从msgTreeMapTemp移除，并将该批消息重新放入msgTreeMap，使这批消息可以被再次消费
    public void makeMessageToCosumeAgain(List<MessageExt> msgs) { // 顺序消费模式下，使消息可以被重新消费
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset()); // 从临时TreeMap中取出消息
                    this.msgTreeMap.put(msg.getQueueOffset(), msg); // 再将消息放到msgTreeMap中
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("makeMessageToCosumeAgain exception", e);
        }
    }

    // 该方法在顺序消费模式下使用，取到消息后，就会调用我们定义的MessageListener进行消费
    public List<MessageExt> takeMessages(final int batchSize) { // 从msgTreeMap中获取消息后：其内部会将消息都放在一个临时的TreeMap中，然后进行顺序消费
        List<MessageExt> result = new ArrayList<MessageExt>(batchSize);
        final long now = System.currentTimeMillis();
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    // 从msgTreeMap中获取batchSize条数据，每次都返回offset最小的那条消息并从msgTreeMap中移除
                    for (int i = 0; i < batchSize; i++) {
                        Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
                        if (entry != null) {
                            result.add(entry.getValue()); // 把消息放到返回列表
                            // 把消息的offset和消息体msg，放到顺序消费TreeMap中
                            consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
                        } else {
                            break;
                        }
                    }
                }
                if (result.isEmpty()) {
                    consuming = false; // 没有取到消息，说明不需要消费，即将consuming置为FALSE。
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("take Messages exception", e);
        }
        return result;
    }

    public boolean hasTempMessage() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                return !this.msgTreeMap.isEmpty();
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
        }

        return true;
    }

    public void clear() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.clear();
                this.consumingMsgOrderlyTreeMap.clear();
                this.msgCount.set(0);
                this.msgSize.set(0);
                this.queueOffsetMax = 0L;
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    public long getLastLockTimestamp() {
        return lastLockTimestamp;
    }

    public void setLastLockTimestamp(long lastLockTimestamp) {
        this.lastLockTimestamp = lastLockTimestamp;
    }

    public Lock getLockConsume() {
        return lockConsume;
    }

    public long getLastPullTimestamp() {
        return lastPullTimestamp;
    }

    public void setLastPullTimestamp(long lastPullTimestamp) {
        this.lastPullTimestamp = lastPullTimestamp;
    }

    public long getMsgAccCnt() {
        return msgAccCnt;
    }

    public void setMsgAccCnt(long msgAccCnt) {
        this.msgAccCnt = msgAccCnt;
    }

    public long getTryUnlockTimes() {
        return this.tryUnlockTimes.get();
    }

    public void incTryUnlockTimes() {
        this.tryUnlockTimes.incrementAndGet();
    }

    public void fillProcessQueueInfo(final ProcessQueueInfo info) {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();

            if (!this.msgTreeMap.isEmpty()) {
                info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
                info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
                info.setCachedMsgCount(this.msgTreeMap.size());
                info.setCachedMsgSizeInMiB((int) (this.msgSize.get() / (1024 * 1024)));
            }

            if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
                info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.firstKey());
                info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.lastKey());
                info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.size());
            }

            info.setLocked(this.locked);
            info.setTryUnlockTimes(this.tryUnlockTimes.get());
            info.setLastLockTimestamp(this.lastLockTimestamp);

            info.setDroped(this.dropped);
            info.setLastPullTimestamp(this.lastPullTimestamp);
            info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
        } catch (Exception e) {
        } finally {
            this.lockTreeMap.readLock().unlock();
        }
    }

    public long getLastConsumeTimestamp() {
        return lastConsumeTimestamp;
    }

    public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
        this.lastConsumeTimestamp = lastConsumeTimestamp;
    }

}
