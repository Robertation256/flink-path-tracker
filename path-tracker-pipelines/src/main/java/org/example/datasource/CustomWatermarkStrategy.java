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

package org.example.datasource;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;


public class CustomWatermarkStrategy<T> implements WatermarkStrategy<DecorateRecord> {

    @Override
    public WatermarkGenerator<DecorateRecord> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<DecorateRecord>() {
            private long maxSequenceNumberSeen = Long.MIN_VALUE;

            @Override
            public void onEvent(
                    DecorateRecord event,
                    long eventTimestamp,
                    WatermarkOutput output) {
                maxSequenceNumberSeen = Math.max(maxSequenceNumberSeen, event.getSeqNum());
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {
                output.emitWatermark(new Watermark(maxSequenceNumberSeen));
            }
        };
    }

    @Override
    public TimestampAssigner<DecorateRecord> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return (event, recordTimestamp) -> event.getSeqNum();
    }
}

