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

package org.example.metric;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.BasicConfigurator;
import org.example.datasource.DecorateRecord;
import org.example.utils.RecordSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileWriter;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

// Consumes DecorateRecord from a Kafka topic and generate a local csv file containing record timestamps
public class MetricConsumer {

    private static final String filename = "output_records.csv";
    private static final Logger LOG = LoggerFactory.getLogger(MetricConsumer.class.getName());


    public static void run(String kafkaServers, String topic) throws  Exception{
        BasicConfigurator.configure();
        LOG.info("Starting metric consumer");

        File file = new File(filename);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();

        FileWriter writer = new FileWriter(filename);
            // Write header
        writer.write("create_time,process_completion_time,sink_time,consume_time,heap_push_time,emit_time\n");

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaServers);
        props.put("group.id", UUID.randomUUID().toString().substring(0, 10));
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        int count = 0;

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(10000));
            if (records.isEmpty()){
                break;
            }
            for (ConsumerRecord<byte[], byte[]> kafkaRecord: records){
                DecorateRecord record = RecordSerdes.fromBytes(kafkaRecord.value());
                writer.write(String.format("%d,%d,%d,%d,%d,%d\n",
                        record.getCreateTime(),
                        record.getProcessCompletionTime(),
                        record.getSinkTime(),
                        record.getConsumeTime(),
                        record.getHeapPushTime(),
                        record.getEmitTime()
                ));
                count++;
            }
        }

        writer.close();

        LOG.info(String.format("Completed writing %d records to file %s", count, filename));
    }

    public static void main(String[] args) throws  Exception {
        String kafkaServers = "localhost:9092";
        String inputTopic = null;

        for (String arg: args){
           if(arg.contains("kafka_server=")){
              kafkaServers = arg.split("=")[1];
           }

            if(arg.contains("topic=")){
                inputTopic = arg.split("=")[1];
            }
        }

        if (inputTopic == null){
            LOG.error("Input topic not specified.");
            return;
        }
        MetricConsumer.run(kafkaServers, inputTopic);
    }
}
