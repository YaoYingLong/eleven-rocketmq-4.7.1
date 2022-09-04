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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }
        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}", consumerGroup, currentCID, cidAll);
            return result;
        }
        int index = cidAll.indexOf(currentCID); // 获取当前客户端ID在消费者集合中的索引位置，cidAll一般是排好序的
        int mod = mqAll.size() % cidAll.size(); // MessageQueue个数 % 消费者个数，判断有多个MessageQueue无法平均分配
        // 计算当前消费者可以分几个队列去消费，
        int averageSize = mqAll.size() <= cidAll.size() ? // 队列个数是否小于等于消费者个数
                1 : // 队列个数小于消费者个数时，一个消费者只能分到一个queue
                (mod > 0 && index < mod ? // 队列个数大于消费者个数时，若queue还剩零头，且当前Client在消费者集合中的索引消费该零头
                        mqAll.size() / cidAll.size() + 1  // 当前client多分配一个queue
                        : mqAll.size() / cidAll.size()); // 没有零头或当前client在消费者集合中的索引大于等于零头，则平均分配或不管零头
        // 计算出集合0位置client在MessageQueue中的开始位置时：0 * 3= 0,1位置：1 * 3 = 3； 2位置：2 * 2 + 2 = 6
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        // 计算出当前消费者可消费queue在MessageQueue集合中的范围
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) { // 将计算出的MessageQueue加入到结果中并返回
            // 当前client所在消费者集合的0位置
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
