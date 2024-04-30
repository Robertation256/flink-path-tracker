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
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.example.merger.KWayMergerConsumer;
import org.example.merger.KafkaMergeThread;
import org.example.metric.MetricConsumer;
import org.example.pipelines.ConfluxPipeline;
import org.example.pipelines.GlobalSortPipeline;
import org.example.utils.KafkaAdminUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.UUID;

public class Main {
    public static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);

        // default to launching conflux if not specified
        boolean runBaseline = Arrays.asList(args).contains("runBaseline");
        boolean runMerger = Arrays.asList(args).contains("runMerger");
        boolean runMetric = Arrays.asList(args).contains("runMetric");
        String flinkTopic = UUID.randomUUID().toString().substring(0,10);
        String mergerTopic = UUID.randomUUID().toString().substring(0,10);
        String metricTopic = "";
        String bootstrapServers = "sp24-cs525-2118.cs.illinois.edu:9092";


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
            if (arg.contains("metric_topic=")){
                metricTopic = arg.split("=")[1];
            }
            if (arg.contains("num_records=")) {
                Configuration.DATASOURCE_SIZE = Long.parseLong(arg.split("=")[1]);
            }
        }


        LOG.info(String.format("Launching with configuration: isBaseline=%b, flink_topic=%s, merger_topic:%s, kafka_server=%s, num_records=%s",runBaseline, flinkTopic, mergerTopic, bootstrapServers, Configuration.DATASOURCE_SIZE));

        if (runBaseline){
            runBaseline(bootstrapServers, flinkTopic);
        }
        else if (runMerger){
            runMerger(bootstrapServers, flinkTopic, mergerTopic);
        }
        else if (runMetric){
            MetricConsumer.run(bootstrapServers, metricTopic);
        }
        else {
            runConflux(bootstrapServers, flinkTopic);
        }
    }

    private static void runBaseline(String kafkaServers, String outputTopic) throws Exception{
        KafkaAdminUtils.createTopic(kafkaServers, outputTopic, 1);
        StreamExecutionEnvironment env = GlobalSortPipeline.create(kafkaServers, outputTopic);
        env.execute();
    }



    private static void runConflux(String kafkaBootstrapServers, String flinkTopic) throws Exception{
        StreamExecutionEnvironment env = ConfluxPipeline.create(kafkaBootstrapServers, flinkTopic);
        try {
            env.execute();
        } catch (Exception e) {
            LOG.error("Encountered error {}", e.toString());
        }
    }


    private static void runMerger(String kafkaBootstrapServers, String flinkTopic, String mergerTopic) throws  Exception{
        int pathNum = ConfluxPipeline.getPathNum();
        LOG.info(String.format("Found %d path in execution graph", pathNum));

        KafkaAdminUtils.createTopic(kafkaBootstrapServers, flinkTopic, pathNum);
        KafkaAdminUtils.createTopic(kafkaBootstrapServers, mergerTopic, 1);


        // Make producer, consumer, and merger
        KafkaMergeThread mergeThread = new KafkaMergeThread(kafkaBootstrapServers, mergerTopic, pathNum);

        Thread merge = new Thread(mergeThread);
        KWayMergerConsumer consumer = new KWayMergerConsumer(kafkaBootstrapServers, flinkTopic, mergeThread.partitionQueue);


        consumer.run();
        merge.start();

        merge.join();
        consumer.stop();
    }

}


