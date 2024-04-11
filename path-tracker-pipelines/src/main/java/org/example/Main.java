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

import com.fasterxml.jackson.databind.annotation.JsonAppend;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.PathAnalyzer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Main {

    // topic for queues
    private static String OUTPUT_TOPIC = "test_topic";

    public static void main(String[] args) throws Exception {
        String bootstrapServers = "localhost:9092";
        String topicName = "test_topic";

        Properties prop = new Properties();
        prop.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        StreamExecutionEnvironment env = createPipeline(bootstrapServers);
        int pathNum = PathAnalyzer.computePathNum(env);

        try (AdminClient adminClient = AdminClient.create(prop)) {
            NewTopic newTopic = new NewTopic(topicName, pathNum, (short) 1);
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            System.out.println("Topic created successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }

        ConcurrentLinkedQueue<kafkaMessage>[] queue = new ConcurrentLinkedQueue[pathNum];
        for(int i = 0; i < pathNum; i++) {
            queue[i] = new ConcurrentLinkedQueue<>();
        }

        // Make producer, consumer, and merger
        KafkaMergeThread mergeThread = new KafkaMergeThread(pathNum, queue);
        KafkaConsumerThread consumeThread = new KafkaConsumerThread(bootstrapServers, pathNum, queue);

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

    private static StreamExecutionEnvironment createPipeline(String kafkaBootstrapServer) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.example.CustomKafkaPartitioner");

        KafkaSink<DecorateRecord<Integer>> kafkaSink = KafkaSink.<DecorateRecord<Integer>>builder()
                .setBootstrapServers(kafkaBootstrapServer)
                .setRecordSerializer(new CustomKafkaSerializer(OUTPUT_TOPIC))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(producerProps)
                .build();

        env.addSource(new TestDataSource(100000)).setParallelism(1)
                // filter out multiples of 7
                .filter(new TestRichFilterFunctionImpl()).setParallelism(3)
                .rescale()
                // multiply by 2
                .map(new TestRichMapFunctionImplForMul2()).setParallelism(4)
                .keyBy(new KeySelector<DecorateRecord<Integer>, Object>() {
                    @Override
                    public Object getKey(DecorateRecord<Integer> record) throws Exception {
                        return record.getValue();
                    }
                })
                // square it
                .map(new TestRichMapFunctionImplForSquare()).setParallelism(2)
                .sinkTo(kafkaSink).setParallelism(1);


        return env;

    }
}



abstract class BaseDecorateRichFunction extends AbstractRichFunction {
    String instanceID;

    @Override
    public void open(Configuration config) {
        int subID = getRuntimeContext().getIndexOfThisSubtask();
        String operatorName = getRuntimeContext().getTaskName();
        instanceID = String.format("%s_%d", operatorName, subID);
    }
}

class DecorateRichFlatMapFunction<IN, OUT> extends BaseDecorateRichFunction implements FlatMapFunction<DecorateRecord<IN>, DecorateRecord<OUT>> {
    @Override
    public void flatMap(
            DecorateRecord<IN> record,
            Collector<DecorateRecord<OUT>> collector) throws Exception {

    }
}

class DecorateRichFilterFunction<IN> extends BaseDecorateRichFunction implements FilterFunction<DecorateRecord<IN>> {
    @Override
    public boolean filter(DecorateRecord<IN> inDecorateRecord) throws Exception {
        return false;
    }
}

class DecorateRichMapFunction<IN, OUT> extends BaseDecorateRichFunction implements MapFunction<DecorateRecord<IN>, DecorateRecord<OUT>> {
    @Override
    public DecorateRecord<OUT> map(DecorateRecord<IN> inDecorateRecord) throws Exception {
        return null;
    }
}

class TestDataSource extends RichSourceFunction<DecorateRecord<Integer>> {
    private boolean running = true;

    private boolean isInfiniteSource = true;
    private long recordsPerInvocation = 0L;
    private long seqNum = 0L;

    public TestDataSource() {
    }

    public TestDataSource(long recordsPerInvocation) {
        this.recordsPerInvocation = recordsPerInvocation;
        this.isInfiniteSource = false;
    }

    @Override
    public void run(SourceContext<DecorateRecord<Integer>> sourceContext) throws Exception {
        int counter = 0;

        long recordsRemaining = this.recordsPerInvocation;
        while (isInfiniteSource || recordsRemaining > 0) {

            sourceContext.collect(new DecorateRecord<Integer>(seqNum++, "", counter++));

            if (!isInfiniteSource) {
                recordsRemaining--;
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}

class TestRichFilterFunctionImpl extends DecorateRichFilterFunction<Integer> {
    @Override
    public boolean filter(DecorateRecord<Integer> record) throws Exception {
        if (record.getValue() % 7 == 0) {
            return false;
        }

        record.addAndSetPathInfo(instanceID);
        return true;
    }
}

class TestRichMapFunctionImplForMul2 extends DecorateRichMapFunction<Integer, Integer> {
    @Override
    public DecorateRecord<Integer> map(DecorateRecord<Integer> record) throws Exception {
        record.addAndSetPathInfo(instanceID);

        record.setValue(record.getValue() * 2);
        return record;
    }
}

class TestRichMapFunctionImplForSquare extends DecorateRichMapFunction<Integer, Integer> {
    @Override
    public DecorateRecord<Integer> map(DecorateRecord<Integer> record) throws Exception {
        record.addAndSetPathInfo(instanceID);

        record.setValue(record.getValue() * record.getValue());
        return record;
    }
}

// use an extra map stage to format output,
// modify the interface of the decorate function in the future
class OutputFormatMap implements MapFunction<DecorateRecord<Integer>, String> {
    @Override
    public String map(DecorateRecord<Integer> value) throws Exception {
        return String.format("%d-%d", value.getSeqNum(), value.getValue());
    }
}
