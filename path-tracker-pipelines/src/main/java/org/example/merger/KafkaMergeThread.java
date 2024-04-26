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
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;


public class KafkaMergeThread extends   Thread {
    
        private static final Logger LOG = LoggerFactory.getLogger(KafkaMergeThread.class.getName());
        private volatile boolean running = true;
        private final int partitionCount;
        public final ConcurrentLinkedQueue<DecorateRecord> [] partitionQueue;
        PriorityQueue<MinHeapTuple> minHeap;
        private final KafkaProducer<byte[], byte[]> kafkaProducer;
        private final String outputTopic;

        int numEvents = 0;
        long startTime;
        boolean [] queuePathIDCount;
        public final ConcurrentHashMap<Integer, Long> watermarks;
        long valuesPopped = 0;
        long lastSeqNum = 0;


    public KafkaMergeThread(String kafkaServers, String outputTopic, int partitionCount) {
            this.partitionCount = partitionCount;
            this.partitionQueue = new ConcurrentLinkedQueue[partitionCount];

            for (int i = 0; i < partitionCount; i++) {
                partitionQueue[i] = new ConcurrentLinkedQueue<>();
            }

            Comparator<MinHeapTuple> tupleComparator = Comparator.comparingLong(t -> t.priority);
            this.minHeap = new PriorityQueue<>(tupleComparator);
            this.watermarks = new ConcurrentHashMap<>();;
            this.queuePathIDCount = new boolean[partitionCount];

            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaServers);
            props.put("key.serializer", ByteArraySerializer.class.getName());
            props.put("value.serializer", ByteArraySerializer.class.getName());

            this.kafkaProducer = new KafkaProducer<>(props);
            this.outputTopic = outputTopic;

    }
        @Override
        public void run() {
            LOG.info("K-way merger merge thread started");
            init_heap(); // Push one value from each path onto the heap
            long lastCheckedWatermark = 0;

            LOG.info("K-way merger heap initialized");

            while (running && !isCompleted()) {
                // Check if watermark has changed
                long currWatermark = getSmallestWatermark();

                if((lastCheckedWatermark != currWatermark) || (minHeap.isEmpty())) {
                    refillHeap(); // Fill up heap with one value from every queue if it exists
                    lastCheckedWatermark = currWatermark;
                } else {
                    refillHeap();
                }



                // Pop smallest item
                if((minHeap.size() == partitionCount)) {
                   emitRecord();
                } else if ((minHeap.peek() !=null) && (minHeap.peek().priority <= currWatermark)) {
                  emitRecord();
                }
            }

            LOG.info("Received final watermark from Flink. Shutting down merge thread.");
            stopRunning();
        }

        public void refillHeap() {
            if(minHeap.size() != partitionCount) {
                for (int queueIdx = 0; queueIdx < partitionCount; queueIdx++) {
                    if(!queuePathIDCount[queueIdx]) { // if a value is not currently in there for this queue
                        ConcurrentLinkedQueue<DecorateRecord> q = partitionQueue[queueIdx];
                        DecorateRecord nextRecord = q.poll();
                        if(nextRecord != null) {
                            nextRecord.setHeapPushTime(Instant.now().toEpochMilli());
                            MinHeapTuple curr = new MinHeapTuple(nextRecord.getSeqNum(), queueIdx, nextRecord);
                            minHeap.add(curr);
                            queuePathIDCount[queueIdx] = true;
                        }
                    }
                }
            }
        }

        private boolean isCompleted(){
            // Flink announce signals a final watermark with Long.MAX_VALUE

            if (!minHeap.isEmpty() || getSmallestWatermark() != Long.MAX_VALUE){
                return false;
            }



            // check if all local queues are drained
            for (ConcurrentLinkedQueue<DecorateRecord> q: partitionQueue){
                if (!q.isEmpty()){
                    return false;
                }
            }

            return true;
        }

        public void init_heap() {
            boolean foundEmptyQueue = false;
            boolean queueInitialized = false;
            startTime = System.currentTimeMillis();

            // Initial queue instantiation
            while(!queueInitialized) {
                for (ConcurrentLinkedQueue<DecorateRecord> q : partitionQueue) {
                    if (q.isEmpty()) {
                        foundEmptyQueue = true;
                        break;
                    }
                }
                if(!foundEmptyQueue) {
                    for (int queueIdx = 0; queueIdx < partitionCount; queueIdx++) {
                        ConcurrentLinkedQueue<DecorateRecord> q = partitionQueue[queueIdx];
                        DecorateRecord record = q.poll();
                        record.setHeapPushTime(Instant.now().toEpochMilli());
                        MinHeapTuple curr = new MinHeapTuple(record.getSeqNum(), queueIdx, record);
                        minHeap.add(curr);
                        queuePathIDCount[queueIdx] = true; // a value from this queue is in the heap
                    }
                    queueInitialized = true;
                }
                foundEmptyQueue = false;
            }
        }

        public void stopRunning() {
            running = false;
            LOG.info("Shutting down K-way merger merge thread. Total number of records popped: " + valuesPopped) ;
        }

        public void emitRecord() {
            MinHeapTuple smallest;
            smallest = minHeap.remove();
            smallest.record.setEmitTime(Instant.now().toEpochMilli());
            if (lastSeqNum > smallest.priority) {
                LOG.error("Spotted safety Violation: last seq num: {}, current seq num: {}",
                        lastSeqNum,
                        smallest.priority);
                stopRunning();
            } else {
                lastSeqNum = smallest.priority;
            }

            kafkaProducer.send(new ProducerRecord<>(outputTopic, RecordSerdes.toBytes(smallest.record)));

             queuePathIDCount[smallest.partitionNumber] = false;
             ConcurrentLinkedQueue<DecorateRecord> q = partitionQueue[smallest.partitionNumber];
             valuesPopped++;

                // Try to refill if possible, if not move on
                DecorateRecord record = q.poll();
                if (record != null) {
                    record.setHeapPushTime(Instant.now().toEpochMilli());
                    MinHeapTuple curr = new MinHeapTuple(
                            record.getSeqNum(),
                            smallest.partitionNumber,
                            record
                    );
                    minHeap.add(curr);
                    queuePathIDCount[smallest.partitionNumber] = true;
                }
        }


        public long getSmallestWatermark() {
            return Collections.min(watermarks.values());
        }

    static class MinHeapTuple {
        long priority;
        int partitionNumber;
        DecorateRecord record;
        public MinHeapTuple(long priority, int partitionNumber, DecorateRecord record) {
            this.priority = priority;
            this.partitionNumber = partitionNumber;
            this.record = record;
            }
        }

    }


