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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;


public class KafkaMergeThread implements  Runnable {
        private volatile boolean running = true;
        private  static final int throughputIntervalMilli = 100;
        private final int partitionCount;
        private final ConcurrentLinkedQueue<kafkaMessage> [] partitionQueue;
        PriorityQueue<minHeapTuple> minHeap;
        List<Tuple<Long, Long>> latencies;
        List<Tuple<Long, Double>> throughput;

        int numEvents = 0;
        long startTime;
        boolean [] queuePathIDCount;
        ConcurrentHashMap<Integer, Long> watermarks;
        long valuesPopped = 0;
        long lastSeqNum = 0;


    public KafkaMergeThread(int partitionCount, ConcurrentLinkedQueue<kafkaMessage>[] queue, ConcurrentHashMap<Integer, Long> watermarks) {
            this.partitionCount = partitionCount;
            this.partitionQueue = queue;
            Comparator<minHeapTuple> tupleComparator = Comparator.comparingLong(t -> t.priority);
            this.minHeap = new PriorityQueue<>(tupleComparator);
            this.latencies = new ArrayList<>();
            this.throughput = new ArrayList<>();
            this.watermarks = watermarks;
            this.queuePathIDCount = new boolean[partitionCount];
    }
        @Override
        public void run() {
            init_heap(); // Push one value from each path onto the heap
            long lastCheckedWatermark = 0;

            while (running) {
                // Check if watermark has changed
                long currWatermark = getSmallestWatermark();
                if((lastCheckedWatermark != currWatermark) || (minHeap.size() == 0)) {
                    if (lastCheckedWatermark != currWatermark) {
                        System.out.println("Smallest watermark: " + currWatermark + "  last checked watermark: " + lastCheckedWatermark);
                    }
                    refillHeap(); // Fill up heap with one value from every queue if it exists
                    lastCheckedWatermark = currWatermark;
                }

                // if len(heap) == pathNum or heap.peek() < watermark
                // Pop smallest item
                if((minHeap.size() == partitionCount)) {
                   emitRecord();
                } else if ((minHeap.peek() !=null) && (minHeap.peek().priority <= lastCheckedWatermark)) {
                  emitRecord();
                }
            }
        }

        public void refillHeap() {
            if(minHeap.size() != partitionCount) {
                for (int queueIdx = 0; queueIdx < partitionCount; queueIdx++) {
                    if(!queuePathIDCount[queueIdx]) { // if a value is not currently in there for this queue
                        ConcurrentLinkedQueue<kafkaMessage> q = partitionQueue[queueIdx];
                        kafkaMessage nextNum = q.poll();
                        if(nextNum != null) {
                            long nextNumUnpacked = nextNum.seqNum;
                            minHeapTuple curr = new minHeapTuple(nextNumUnpacked, q, nextNum.arrivalTime, queueIdx);
                            minHeap.add(curr);
                            queuePathIDCount[queueIdx] = true;
                        }
                    }
                }
            }
        }

        public void init_heap() {
            boolean foundEmptyQueue = false;
            boolean queueInitialized = false;
            startTime = System.currentTimeMillis();

            // Initial queue instantiation
            while(!queueInitialized) {
                for (ConcurrentLinkedQueue<kafkaMessage> q : partitionQueue) {
                    if (q.size() == 0) {
                        foundEmptyQueue = true;
                        break;
                    }
                }
                if(!foundEmptyQueue) {
                    for (int queueIdx = 0; queueIdx < partitionCount; queueIdx++) {
                        ConcurrentLinkedQueue<kafkaMessage> q = partitionQueue[queueIdx];
                        kafkaMessage temp = q.poll();
                        minHeapTuple curr = new minHeapTuple(temp.seqNum, q, temp.arrivalTime, queueIdx);
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
            statistics.uploadLatencyToFile("latency.txt", latencies);
            statistics.uploadThroughputToFile("throughput.txt", throughput);
            System.out.println("Number of values popped " + valuesPopped) ;
        }

        public void emitRecord() {
            minHeapTuple smallest;
            smallest = minHeap.remove();
            if (lastSeqNum > smallest.priority) {
                System.out.println("========= \n Safety Violation \n ========== " + "Last seq Number: " + lastSeqNum + " Curr Seq Number " + smallest.priority);
                stopRunning();
            } else {
                lastSeqNum = smallest.priority;
            }

             queuePathIDCount[smallest.partitionNumber] = false;
             ConcurrentLinkedQueue<kafkaMessage> q = smallest.q;
             updateStatistics(smallest);
             valuesPopped++;

                // Try to refill if possible, if not move on
                kafkaMessage nextNum = q.poll();
                if (nextNum != null) {
                    long nextNumUnpacked = nextNum.seqNum;
                    minHeapTuple curr = new minHeapTuple(
                            nextNumUnpacked,
                            q,
                            nextNum.arrivalTime,
                            smallest.partitionNumber);
                    minHeap.add(curr);
                    queuePathIDCount[smallest.partitionNumber] = true;
                }
        }

        public void updateStatistics(minHeapTuple poppedValue) {
            long processingTime = System.currentTimeMillis() - poppedValue.createTime;
            this.latencies.add(new Tuple<>(System.currentTimeMillis(), processingTime)) ;
            numEvents++;
            long totalTime = (System.currentTimeMillis() - startTime) / 1000 ;
            double throughput_curr = (double) numEvents / totalTime;
            this.throughput.add(new Tuple<>(System.currentTimeMillis(),throughput_curr));
        }

        public long getSmallestWatermark() {
            return Collections.min(watermarks.values());
        }

    static class minHeapTuple{
        long priority;
        ConcurrentLinkedQueue<kafkaMessage> q;
        long createTime;
        int partitionNumber;
        public minHeapTuple(long priority, ConcurrentLinkedQueue<kafkaMessage> queue, long time, int partitionNumber) {
            this.priority = priority;
            this.q = queue; // No longer needed as we can just index into the queue list using partitionNumber but will leave for now
            this.createTime = time;
            this.partitionNumber = partitionNumber;
            }
        }

    public static class Tuple<K, V> {
        K first;
        V second;

        public Tuple(K first, V second) {
            this.first = first;
            this.second = second;
        }
    }
    }


    class statistics {
        public static void uploadThroughputToFile(String filename, List<KafkaMergeThread.Tuple<Long, Double>> data) {
            try {
                File file = new File(filename);
                if (!file.exists()) {
                    file.createNewFile();
                }

                try (FileWriter writer = new FileWriter(filename)) {
                    // Write header
                    writer.write("Timestamp,Value\n");

                    // Write data to file
                    for (KafkaMergeThread.Tuple<Long, ?> tuple : data) {
                        writer.write(tuple.first + "," + tuple.second + "\n");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public static void uploadLatencyToFile(String filename, List<KafkaMergeThread.Tuple<Long, Long>> data) {
            try {
                File file = new File(filename);
                if (!file.exists()) {
                    file.createNewFile();
                }

                try (FileWriter writer = new FileWriter(filename)) {
                    // Write header
                    writer.write("Timestamp,Value\n");

                    // Write data to file
                    for (KafkaMergeThread.Tuple<Long, ?> tuple : data) {
                        writer.write(tuple.first + "," + tuple.second + "\n");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

}


