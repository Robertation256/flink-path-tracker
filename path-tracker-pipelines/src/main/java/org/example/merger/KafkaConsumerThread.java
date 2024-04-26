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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.example.datasource.DecorateRecord;
import org.example.utils.RecordSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public class KafkaConsumerThread extends Thread{

        private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerThread.class);
        private volatile boolean running = true;
        private final Consumer<byte[], byte []> consumer;
        private final BlockingQueue<DecorateRecord> [] partitionQueue;
        long recordsReceived = 0;


    public KafkaConsumerThread(String bootstrapServer , BlockingQueue<DecorateRecord>[] queue, String topicName) {
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_id");
            consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100000);
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.consumer = new KafkaConsumer<>(consumerProps);
            this.consumer.subscribe(Collections.singletonList(topicName));
            this.partitionQueue = queue;
        }

        @Override
        public void run() {
            while (running) {
                int pollTimeMilli = 100;
                ConsumerRecords<byte [], byte []> consumerRecords = consumer.poll(Duration.ofMillis(pollTimeMilli));
                for (ConsumerRecord<byte [], byte []> consumerRecord : consumerRecords) {
                    DecorateRecord record = RecordSerdes.fromBytes(consumerRecord.value());
                    record.setConsumeTime(Instant.now().toEpochMilli());
                    partitionQueue[consumerRecord.partition()].offer(record);
                    if (!record.isDummyWatermark()){
                        recordsReceived++;
                    }
                }
                consumer.commitSync();
            }
            consumer.close();
        }

        public void stopRunning() {
            LOG.info("Shutting down K-way merger consumer thread. Total number of records received: {}", recordsReceived);
            running = false;
        }

}
