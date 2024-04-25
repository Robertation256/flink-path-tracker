/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example.merger;

import com.esotericsoftware.minlog.Log;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.example.datasource.DecorateRecord;
import org.example.utils.RecordSerdes;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class KafkaConsumerThread extends Thread{
        private volatile boolean running = true;
        private int partitionCount;
        Consumer<byte[], byte []> consumer;
        private final ConcurrentLinkedQueue<DecorateRecord> [] partitionQueue;
        ConcurrentHashMap<Integer, Long> watermarks;
        long recordsReceived = 0;


    public KafkaConsumerThread(String bootstrapServer , int partitionCount, ConcurrentLinkedQueue<DecorateRecord>[] queue,  ConcurrentHashMap<Integer, Long> watermarks, String topicName) {
            this.partitionCount = partitionCount;
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_id");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.consumer = new KafkaConsumer<>(consumerProps);
            this.consumer.subscribe(Collections.singletonList(topicName));
            this.partitionQueue = queue;
            this.watermarks = watermarks;

            for(int i = 0; i < partitionCount; i++) {
                watermarks.put(i, 0L); // Initialize all watermarks
            }
        }
        @Override
        public void run() {
            while (running) {
                int pollTimeMilli = 100;
                ConsumerRecords<byte [], byte []> consumerRecords = consumer.poll(Duration.ofMillis(pollTimeMilli));
                for (ConsumerRecord<byte [], byte []> consumerRecord : consumerRecords) {
                    DecorateRecord record = RecordSerdes.fromBytes(consumerRecord.value());
                    record.setConsumeTime(Instant.now().toEpochMilli());
                    if (record.isDummyWatermark()) {
                        updateWatermark(record.getSeqNum(), consumerRecord.partition());
                    } else {
//                        updateWatermark(record.getSeqNum(), consumerRecord.partition());
                        recordsReceived++;
                        partitionQueue[consumerRecord.partition()].offer(record);
                    }
                    }
                consumer.commitSync();
                }
            close();
        }

        public void updateWatermark(long watermarkValue, int pathID) {
            if(watermarks.get(pathID) < watermarkValue) {
                watermarks.put(pathID, watermarkValue);
            }
        }

        public void stopRunning() {
            running = false;
            Log.info("Shutting down K-way merger consumer thread. Total number of records received: " + recordsReceived);
        }
        public void close() {
            this.consumer.close();
        }
}
