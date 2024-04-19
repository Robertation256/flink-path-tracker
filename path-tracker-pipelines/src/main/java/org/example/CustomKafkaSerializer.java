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


public class CustomKafkaSerializer implements
        KafkaRecordSerializationSchema<DecorateRecord> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CustomKafkaSerializer.class);

    private final String topic;


    public CustomKafkaSerializer(String topic)
    {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            DecorateRecord element, KafkaSinkContext context, Long timestamp) {



//        LOG.info(String.format("Emitting record %s", element));


        if (element.getQueueId() < 0){
            throw new IllegalArgumentException(String.format("Queue id not assigned for record %s", element));
        }



        try {
            return new ProducerRecord<>(
                    topic,
                    Ints.toByteArray(element.getQueueId()) ,
                    RecordSerdes.toBytes(element)
            );
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Could not serialize record: " + element, e);
        }
    }
}
