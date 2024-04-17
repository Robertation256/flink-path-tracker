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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import javax.swing.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.example.metric.HistogramExample;


public class KafkaMergeThread implements  Runnable {
        private volatile boolean running = true;
        private  static final int throughputIntervalMilli = 100;
        private final int partitionCount;
        private final ConcurrentLinkedQueue<kafkaMessage> [] partitionQueue;
        PriorityQueue<minHeapTuple> minHeap;
        public ArrayList<Long> latencies;
        public ArrayList<Double> throughput;
        AtomicLong watermark;
        int numEvents = 0;
        long startTime;
        boolean [] queuePathIDCount;

    public KafkaMergeThread(int partitionCount, ConcurrentLinkedQueue<kafkaMessage>[] queue, AtomicLong watermark) {
            this.partitionCount = partitionCount;
            this.partitionQueue = queue;
            Comparator<minHeapTuple> tupleComparator = Comparator.comparingLong(t -> t.priority);
            this.minHeap = new PriorityQueue<>(tupleComparator);
            this.latencies = new ArrayList<>();
            this.throughput = new ArrayList<>();
            this.watermark = watermark;
            this.queuePathIDCount = new boolean[partitionCount];
    }
        @Override
        public void run() {
            long lastCheckedWatermark = 0;
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

            while (running) {
                // Check if watermark has changed
                // Try to refill all queues which don't have a value already in the heap
                long currWatermark = watermark.get();
                if(lastCheckedWatermark != currWatermark || minHeap.size() == 0) {
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
                    lastCheckedWatermark = currWatermark;
                }
                minHeapTuple smallest;

                // if len(heap) == pathNum or heap.peek() < watermark
                // Pop smallest item
                if(minHeap.size() == partitionCount || (minHeap.peek() !=null && minHeap.peek().priority <= lastCheckedWatermark)) {
                    smallest = minHeap.remove();
                    queuePathIDCount[smallest.partitionNumber] = false;
                    ConcurrentLinkedQueue<kafkaMessage> q = smallest.q;
                    updateStatistics(smallest);

                    // Try to refill if possible, if not move on
                    kafkaMessage nextNum = q.poll();
                    if(nextNum != null) {
                        long nextNumUnpacked = nextNum.seqNum;
                        minHeapTuple curr = new minHeapTuple(nextNumUnpacked, q, nextNum.arrivalTime, smallest.partitionNumber);
                        minHeap.add(curr);
                        queuePathIDCount[smallest.partitionNumber] = true;
                    }
                }

                // System.out.print(sequenceNum + " "); // uncomment to verify correctness

            }
        }
        public void stopRunning() {
            running = false;
            statistics.getDescriptiveStats(latencies, throughput);
            System.out.println("Latency Values " + this.latencies.size());
            System.out.println("Throughput values: " + this.throughput.size());
        }
        public void updateStatistics(minHeapTuple poppedValue) {
            long processingTime = System.currentTimeMillis() - poppedValue.createTime;
            latencies.add(processingTime);
            numEvents++;
            long totalTime = (System.currentTimeMillis() - startTime) / 1000 ;
            double throughput_curr = (double) numEvents / totalTime;
            this.throughput.add(throughput_curr);
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
    }

    class statistics {
        public static void getDescriptiveStats(ArrayList<Long> latency, ArrayList<Double> throughput)    {
            long [] latencyArray = new long[latency.size()];
            double [] throughputArray = new double[throughput.size()];

            DescriptiveStatistics throughputStats = new DescriptiveStatistics();
            for(int i = 0; i < throughput.size(); i++) {
                throughputStats.addValue(throughput.get((i)));
                throughputArray[i] = throughput.get(i);
            }
            DescriptiveStatistics latencyStats = new DescriptiveStatistics();

            for(int i = 0; i < latency.size(); i++) {
                latencyStats.addValue(latency.get((i)));
                latencyArray[i] = latency.get(i);
            }

            System.out.println("Throughput Mean: " + throughputStats.getMean());
            System.out.println("Throughput Standard Deviation: " + throughputStats.getStandardDeviation());
            System.out.println("Throughput Variance: " + throughputStats.getVariance());

            System.out.println("Latency Mean: " + latencyStats.getMean());
            System.out.println("Latency Standard Deviation: " + latencyStats.getStandardDeviation());
            System.out.println("Latency Variance: " + latencyStats.getVariance());

            int numberOfBins = 20; // Number of bins for histogram

            // Create and display the histogram
            SwingUtilities.invokeLater(() -> {
                HistogramExample example = new HistogramExample("Conflux throughput", throughputArray, numberOfBins, 16);
                example.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
                example.pack();
                example.setVisible(true);
            });

            SwingUtilities.invokeLater(() -> {
                HistogramExample example = new HistogramExample("Conflux per record latency", latencyArray, numberOfBins, 25);
                example.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
                example.pack();
                example.setVisible(true);
            });

        }
}


