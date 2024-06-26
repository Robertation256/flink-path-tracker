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

package org.example.pipelines;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.PathAnalyzer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.example.CustomKafkaSerializer;
import org.example.datasource.CustomWatermarkStrategy;
import org.example.datasource.DecorateRecord;
import org.example.datasource.TestDataSource;
import org.example.operator.CustomWatermarkProcessor;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConfluxPipeline {

    private static Map<String, Integer> pathIdToQueueId = null;

    public static StreamExecutionEnvironment create(String kafkaBootstrapServer, String recordOutputTopic) throws Exception{
        pathIdToQueueId = new HashMap<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        config.set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ofMillis(org.example.Configuration.WATERMARK_EMISSION_PERIOD_MILLIS));
        env.configure(config);


        WatermarkStrategy<DecorateRecord> customWatermarkStrategy = new CustomWatermarkStrategy<>();

        DataStream<DecorateRecord> datasource = env
                .addSource(new TestDataSource(org.example.Configuration.DATASOURCE_SIZE)).setParallelism(1)
                .assignTimestampsAndWatermarks(customWatermarkStrategy).setParallelism(1);
        DataStream<DecorateRecord> recordStream = Workload.attachTestPipeline(datasource);



        CustomWatermarkProcessor watermarkProcessor = new CustomWatermarkProcessor();

        int upstreamParallelism = recordStream.getParallelism();
        DataStream<DecorateRecord> streamWithWatermark = recordStream
                .forward()
                .transform("watermark-extraction",
                        TypeInformation.of(new TypeHint<DecorateRecord>() {}),
                        watermarkProcessor
                ).setParallelism(upstreamParallelism);




        streamWithWatermark
                .forward()
                .sinkTo(getRecordSink(kafkaBootstrapServer, recordOutputTopic))
                .setParallelism(upstreamParallelism);


        List<String> pathIds = PathAnalyzer.computePathIDs(env);
        for (int i=0; i<pathIds.size(); i++){
            pathIdToQueueId.put(pathIds.get(i), i);
        }

        watermarkProcessor.setPathIds(pathIds);

        return env;
    }

    public static int getPathNum() throws  Exception{
        if (pathIdToQueueId == null){
            create("", "");
        }
        return pathIdToQueueId.size();
    }

    private static KafkaSink<DecorateRecord> getRecordSink(String kafkaServer, String topic){
        Properties producerProps = new Properties();
//        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.example.utils.CustomKafkaPartitioner");

        return KafkaSink.<DecorateRecord>builder()
                .setBootstrapServers(kafkaServer)
                .setRecordSerializer(new CustomKafkaSerializer(topic, pathIdToQueueId))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(producerProps)
                .build();

    }


    public static void main(String[] args) throws  Exception{
        create("", "");
    }
}
