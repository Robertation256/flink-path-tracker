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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.example.datasource.DecorateRecord;
import org.example.utils.RecordSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class KWayMergerConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KWayMergerConsumer.class);
    private static AtomicLong totalRecordReceived = new AtomicLong();
    private final BlockingQueue<DecorateRecord> [] queues;
    private final String kafkaServer;
    private final String flinkTopic;
    private ConsumeThread[] threads;


    public KWayMergerConsumer(String kafkaServers, String flinkTopic, BlockingQueue<DecorateRecord> [] queues){
        this.queues = queues;
        this.kafkaServer = kafkaServers;
        this.flinkTopic = flinkTopic;
        this.threads = new ConsumeThread[queues.length];
    }


    public void run() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString().substring(0, 10));
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        for (int queueId=0; queueId<queues.length; queueId++){
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps);
            consumer.assign(Collections.singletonList(new TopicPartition(flinkTopic, queueId)));
            ConsumeThread thread = new ConsumeThread(queues, consumer);
            threads[queueId] = thread;
            thread.start();
        }
    }

    public void stop() throws Exception {
        for (ConsumeThread thread: threads){
            thread.running = false;
            thread.join();
        }
        LOG.info("Shutting down K-way merger consumer thread. Total number of records received: {}", totalRecordReceived.get());
    }



    private static class ConsumeThread  extends  Thread {
        private volatile boolean running = true;
        private final BlockingQueue<DecorateRecord>[] queues;
        private final KafkaConsumer<byte [], byte []> consumer;
        public ConsumeThread(BlockingQueue<DecorateRecord>[] queues, KafkaConsumer<byte [], byte []> consumer){
            // we only know which kafka partition we are assigned to during runtime thus we need to reference the whole array
            this.queues = queues;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            long recordsReceived = 0;
            int queueId = -1;
            while (running) {
                int pollTimeMilli = 100;
                ConsumerRecords<byte [], byte []> consumerRecords = consumer.poll(Duration.ofMillis(pollTimeMilli));
                for (ConsumerRecord<byte [], byte []> consumerRecord : consumerRecords) {
                    DecorateRecord record = RecordSerdes.fromBytes(consumerRecord.value());
                    record.setConsumeTime(Instant.now().toEpochMilli());

                    if (queueId>=0 && queueId!=consumerRecord.partition()){
                        LOG.error("Received records from different Kafka partition");
                        return;
                    }
                    queueId = consumerRecord.partition();

                    queues[consumerRecord.partition()].offer(record);
                    if (!record.isDummyWatermark()){
                        recordsReceived++;
                    }
                }
                consumer.commitSync();
            }
            consumer.close();
            totalRecordReceived.addAndGet(recordsReceived);
        }
    }
}
