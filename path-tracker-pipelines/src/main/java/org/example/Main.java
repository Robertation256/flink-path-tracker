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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.example.merger.KafkaConsumerThread;
import org.example.merger.KafkaMergeThread;
import org.example.merger.kafkaMessage;
import org.example.pipelines.ConfluxPipeline;
import org.example.pipelines.GlobalSortPipeline;
import org.example.utils.KafkaAdminUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Main {

    public static void main(String[] args) throws Exception {


        String outputTopic = "test_topic";
        // runBaseline();

//         runConfluxWithKafkaContainer(outputTopic);

        // alternatively run with local kafka instance
        String bootstrapServers = "localhost:9092";
        runConflux(bootstrapServers, outputTopic);

    }

    private static void runBaseline() throws Exception{
        StreamExecutionEnvironment env = GlobalSortPipeline.create();
        env.execute();
    }


    private static void runConfluxWithKafkaContainer(String outputTopic) throws Exception {
        KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
        kafka.start();
        runConflux(kafka.getBootstrapServers(), outputTopic);
    }


    private static void runConflux(String kafkaBootstrapServers, String outputTopic) throws Exception{

        StreamExecutionEnvironment env = ConfluxPipeline.create(kafkaBootstrapServers, outputTopic);
        int pathNum = ConfluxPipeline.getPathNum();
        KafkaAdminUtils.createTopic(kafkaBootstrapServers, outputTopic, pathNum);



        //todo: move K-way merger initialization to a separate start() function

        ConcurrentLinkedQueue<kafkaMessage>[] queue = new ConcurrentLinkedQueue[pathNum];
        for (int i = 0; i < pathNum; i++) {
            queue[i] = new ConcurrentLinkedQueue<>();
        }


        ConcurrentHashMap<Integer, Long> watermarks = new ConcurrentHashMap<>();

        // Make producer, consumer, and merger
        KafkaMergeThread mergeThread = new KafkaMergeThread(pathNum, queue, watermarks);
        KafkaConsumerThread consumeThread = new KafkaConsumerThread(kafkaBootstrapServers, pathNum, queue, watermarks);

        Thread merge = new Thread(mergeThread);
        Thread consume = new Thread(consumeThread);

        merge.setDaemon(true);
        consume.setDaemon(true);

        consume.start();
        merge.start();

        try {
            env.execute();
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Closing Consumer and producer");
            consumeThread.stopRunning();

            System.out.println("Stopping merge");
            mergeThread.stopRunning();
        }

    }
}


