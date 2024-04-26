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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.example.datasource.DecorateRecord;
import org.example.utils.RecordSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Instant;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;


public class KafkaMergeThread extends   Thread {
    
        private static final Logger LOG = LoggerFactory.getLogger(KafkaMergeThread.class.getName());
        public final BlockingQueue<DecorateRecord> [] partitionQueue;
        private final PriorityQueue<MinHeapTuple> minHeap;
        private final KafkaProducer<byte[], byte[]> kafkaProducer;
        private final String outputTopic;


    public KafkaMergeThread(String kafkaServers, String outputTopic, int partitionCount) {
            this.partitionQueue = new BlockingQueue[partitionCount];

            for (int i = 0; i < partitionCount; i++) {
                partitionQueue[i] = new LinkedBlockingDeque<>();
            }

            Comparator<MinHeapTuple> tupleComparator = new Comparator<MinHeapTuple>() {
                @Override
                public int compare(MinHeapTuple o1, MinHeapTuple o2) {
                    // make sure watermark is always larger than the record with the same seq num
                    if ( (o1.record.isDummyWatermark()!=o2.record.isDummyWatermark()) && (o1.record.getSeqNum() == o2.record.getSeqNum())){
                        return o1.record.isDummyWatermark()?1:-1;
                    }

                    return Long.compare(o1.record.getSeqNum(), o2.record.getSeqNum());
                }
            };
            this.minHeap = new PriorityQueue<>(tupleComparator);

            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaServers);
            props.put("key.serializer", ByteArraySerializer.class.getName());
            props.put("value.serializer", ByteArraySerializer.class.getName());

            this.kafkaProducer = new KafkaProducer<>(props);
            this.outputTopic = outputTopic;

    }

    @Override
    public void run() {
        long prevSeqNum = -1L;
        long recordsEmitted = 0L;
        LOG.info("K-way merger merge thread started");

        try {
            for (int i=0; i<partitionQueue.length; i++) {
                minHeap.offer(new MinHeapTuple(i, partitionQueue[i].take()));
            }
        } catch (Exception e){
            LOG.error("Failed to initialize heap with error: {}", e.toString());
        }
        LOG.info("K-way merger heap initialized");

        try{

            while (!minHeap.isEmpty()) {
                MinHeapTuple tuple = minHeap.remove();
                int queueId = tuple.partitionNumber;
                DecorateRecord record = tuple.record;

                // pop
                if (!record.isDummyWatermark()){
                    record.setEmitTime(Instant.now().toEpochMilli());
                    if (record.getSeqNum() < prevSeqNum){
                        LOG.error("Spotted safety violation: prev_seq_num: {}, curr_seq_num: {}", prevSeqNum, record.getSeqNum());
                        return;
                    }
                    prevSeqNum = record.getSeqNum();
                    recordsEmitted++;
                    kafkaProducer.send(new ProducerRecord<>(outputTopic, RecordSerdes.toBytes(record)));
                }

                //refill
                if (record.getSeqNum() < Long.MAX_VALUE){
                    minHeap.add(new MinHeapTuple(queueId, partitionQueue[queueId].take()));
                }
            }

        } catch (Exception e){
            LOG.error("Merger crashed with error {}", e.toString());
        }



        LOG.info("Received final watermark from Flink. Shutting down merge thread.");
        LOG.info("Total number of records popped: {}", recordsEmitted) ;

    }
        



    static class MinHeapTuple {
        int partitionNumber;
        DecorateRecord record;
        public MinHeapTuple(int partitionNumber, DecorateRecord record) {
            this.partitionNumber = partitionNumber;
            this.record = record;
            }
        }

    }


