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

import com.esotericsoftware.minlog.Log;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.example.datasource.DecorateRecord;
import org.example.merger.KafkaConsumerThread;
import org.example.merger.KafkaMergeThread;
import org.example.merger.kafkaMessage;
import org.example.pipelines.ConfluxPipeline;
import org.example.pipelines.GlobalSortPipeline;
import org.example.utils.KafkaAdminUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Main {
    public static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);

        // default to launching conflux if not specified
        boolean runBaseline = Arrays.asList(args).contains("runBaseline");
        String flinkTopic = UUID.randomUUID().toString().substring(0,10);
        String mergerTopic = UUID.randomUUID().toString().substring(0,10);
        String bootstrapServers = "localhost:9092";

        for (String arg: args){
            if (arg.contains("flink_topic=")){
                flinkTopic = arg.split("=")[1];
            }

            if (arg.contains("merger_topic=")){
                mergerTopic = arg.split("=")[1];
            }

            if (arg.contains("kafka_server=")){
                bootstrapServers = arg.split("=")[1];
            }

        }


        LOG.info(String.format("Launching with configuration: isBaseline=%b, flink_topic=%s, merger_topic:%s, kafka_server=%s",runBaseline, flinkTopic, mergerTopic, bootstrapServers));


        if (runBaseline){
            runBaseline();
        }
        else {
            runConflux(bootstrapServers, flinkTopic, mergerTopic);
        }
    }

    private static void runBaseline() throws Exception{
        StreamExecutionEnvironment env = GlobalSortPipeline.create();
        env.execute();
    }



    private static void runConflux(String kafkaBootstrapServers, String flinkTopic, String mergerTopic) throws Exception{

        StreamExecutionEnvironment env = ConfluxPipeline.create(kafkaBootstrapServers, flinkTopic);
        int pathNum = ConfluxPipeline.getPathNum();
        Log.info(String.format("Found %d path in execution graph", pathNum));

        KafkaAdminUtils.createTopic(kafkaBootstrapServers, flinkTopic, pathNum);
        KafkaAdminUtils.createTopic(kafkaBootstrapServers, mergerTopic, 1);

        //todo: move K-way merger initialization to a separate start() function


        // Make producer, consumer, and merger
        KafkaMergeThread mergeThread = new KafkaMergeThread(kafkaBootstrapServers, mergerTopic, pathNum);
        KafkaConsumerThread consumeThread = new KafkaConsumerThread(kafkaBootstrapServers, pathNum, mergeThread.partitionQueue, mergeThread.watermarks, flinkTopic);

        Thread merge = new Thread(mergeThread);
        Thread consume = new Thread(consumeThread);

        merge.setDaemon(true);
        consume.setDaemon(true);

        consume.start();
        merge.start();

        try {
            env.execute();
            mergeThread.join();
            consumeThread.stopRunning();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


