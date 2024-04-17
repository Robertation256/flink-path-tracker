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


import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.datasource.DecorateRecord;
import org.jetbrains.annotations.Nullable;
import org.testcontainers.shaded.org.bouncycastle.util.Strings;


public class WatermarkKafkaSerializer<T> implements
        KafkaRecordSerializationSchema<DecorateRecord<T>> {

    private static final long serialVersionUID = 1L;

    private String topic;


    public WatermarkKafkaSerializer(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            DecorateRecord<T> element,
            KafkaSinkContext kafkaSinkContext,
            Long aLong) {
        try {
            return new ProducerRecord<>(
                    topic,
                    Strings.toByteArray("watermark"),
                    Strings.toByteArray(String.format("%d", element.getSeqNum())));
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Could not serialize watermark record: " + element, e);
        }
    }
}
