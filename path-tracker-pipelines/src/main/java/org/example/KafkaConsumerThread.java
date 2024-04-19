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

package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.datasource.DecorateRecord;
import org.example.utils.RecordSerdes;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class KafkaConsumerThread implements Runnable{
        private volatile boolean running = true;
        private int partitionCount;
        Consumer<String, String> consumer;
        private final ConcurrentLinkedQueue<kafkaMessage> [] partitionQueue;
        ConcurrentHashMap<Integer, Long> watermarks;
        long recordsReceived = 0;


    public KafkaConsumerThread(String bootstrapServer , int partitionCount, ConcurrentLinkedQueue<kafkaMessage>[] queue,  ConcurrentHashMap<Integer, Long> watermarks) {
            this.partitionCount = partitionCount;
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_id");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            this.consumer = new KafkaConsumer<>(consumerProps);
            this.consumer.subscribe(Collections.singletonList("test_topic"));
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
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeMilli));
                for (ConsumerRecord<String, String> record : records) {
                    byte[] record_val = record.value().getBytes(StandardCharsets.UTF_8);
                    DecorateRecord curr_record = RecordSerdes.fromBytes(record_val);
                    if (curr_record.isDummyWatermark()) {
                        updateWatermark(curr_record.getSeqNum(), record.partition());
                    } else {
                        recordsReceived++;
                        kafkaMessage message = new kafkaMessage(
                                curr_record.getSeqNum(),
                                1,
                                System.currentTimeMillis());
                        partitionQueue[record.partition()].offer(message);
                    }
                    }
                consumer.commitSync();
                }
            close();
        }

        public void updateWatermark(long watermarkValue, int pathID) {
            if(watermarks.get(pathID) < watermarkValue) {
                System.out.println(
                        "For path: " + pathID + " Updating watermark to " + watermarkValue);
                watermarks.put(pathID, watermarkValue);
            }
        }

        public void stopRunning() {
            running = false;
            System.out.println("Records received " + recordsReceived);
        }
        public void close() {
            this.consumer.close();
        }
}
