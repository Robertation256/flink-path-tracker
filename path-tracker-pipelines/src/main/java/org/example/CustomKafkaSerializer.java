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


import com.google.common.primitives.Ints;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.datasource.DecorateRecord;
import org.example.utils.RecordSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;


public class CustomKafkaSerializer implements
        KafkaRecordSerializationSchema<DecorateRecord> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CustomKafkaSerializer.class);

    private final String topic;
    private  Map<String, Integer> pathIdToQueueId = null;


    public CustomKafkaSerializer(String topic)
    {
        this.topic = topic;
    }

    public CustomKafkaSerializer(String topic, Map<String, Integer> pathIdToQueueId)
    {
        this.topic = topic;
        this.pathIdToQueueId = pathIdToQueueId;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            DecorateRecord element, KafkaSinkContext context, Long timestamp) {


        try {

            if (pathIdToQueueId != null){
                if (!pathIdToQueueId.containsKey(element.getPathInfo())){
                    LOG.error("Failed to decide queue for record: {}", element);
                    System.exit(1);
                }
                element.setQueueId(pathIdToQueueId.get(element.getPathInfo()));
            }

            element.setSinkTime(Instant.now().toEpochMilli());

            return new ProducerRecord<>(
                    topic,
                    element.getQueueId(),
                    Ints.toByteArray(element.getQueueId()) ,
                    RecordSerdes.toBytes(element)
            );
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Could not serialize record: " + element, e);
        }
    }
}
