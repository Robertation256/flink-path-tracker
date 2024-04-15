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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.PathAnalyzer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.example.datasource.DecorateRecord;
import org.example.operator.TestRichFilterFunctionImpl;
import org.example.operator.TestRichMapFunctionImplForMul2;
import org.example.operator.TestRichMapFunctionImplForSquare;
import org.example.pipelines.ConfluxPipeline;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Main {

    public static void main(String[] args) throws Exception {


        String outputTopic = "test_topic";

        runConfluxWithKafkaContainer(outputTopic);

        // alternatively run with local kafka instance
//        String bootstrapServers = "localhost:9092";
//        runConflux(bootstrapServers, outputTopic);

    }


    private static void runConfluxWithKafkaContainer(String outputTopic) throws Exception {
        KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
        kafka.start();
        runConflux(kafka.getBootstrapServers(), outputTopic);

    }


    private static void runConflux(String kafkaBootstrapServers, String outputTopic) throws Exception{

        StreamExecutionEnvironment env = ConfluxPipeline.create(kafkaBootstrapServers, outputTopic);
        int pathNum = PathAnalyzer.computePathNum(env);


        Properties prop = new Properties();
        prop.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);

        try (AdminClient adminClient = AdminClient.create(prop)) {
            NewTopic newTopic = new NewTopic(kafkaBootstrapServers, pathNum, (short) 1);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            System.out.println("Topic created successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }


        //todo: move K-way merger initialization to a separate start() function

        ConcurrentLinkedQueue<kafkaMessage>[] queue = new ConcurrentLinkedQueue[pathNum];
        for(int i = 0; i < pathNum; i++) {
            queue[i] = new ConcurrentLinkedQueue<>();
        }

        // Make producer, consumer, and merger
        KafkaMergeThread mergeThread = new KafkaMergeThread(pathNum, queue);
        KafkaConsumerThread consumeThread = new KafkaConsumerThread(kafkaBootstrapServers, pathNum, queue);

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


