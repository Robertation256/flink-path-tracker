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
package org.example.operator;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.util.OutputTag;

import org.example.datasource.DecorateRecord;


public class CustomWatermarkProcessor<T> extends AbstractStreamOperator<DecorateRecord<T>> implements OneInputStreamOperator<DecorateRecord<T>, DecorateRecord<T>> {
    private String instanceID;
    private final OutputTag<DecorateRecord<T>> watermarkTag;

    public CustomWatermarkProcessor(OutputTag<DecorateRecord<T>> watermarkTag) {
        this.watermarkTag = watermarkTag;
    }

    @Override
    public void open() throws Exception {
        int subID = getRuntimeContext().getIndexOfThisSubtask();
        String operatorName = getRuntimeContext().getTaskName();
        instanceID = String.format("%s_%d", operatorName, subID);
    }

    @Override
    public void processElement(StreamRecord<DecorateRecord<T>> element) throws  Exception {
        output.collect(element);
    }

    @Override
    public void processWatermark(org.apache.flink.streaming.api.watermark.Watermark mark) throws Exception {
        System.out.printf("Received Watermark:%s", mark.getTimestamp());
        output.collect(watermarkTag, new StreamRecord<>(new DecorateRecord<>(mark.getTimestamp(), true)));
        output.emitWatermark(mark);
    }
}
