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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import org.apache.flink.util.Collector;

import org.example.datasource.DecorateRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueIdAssigner extends ProcessFunction<DecorateRecord, DecorateRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(QueueIdAssigner.class);

    private final int partitionNum;

    private MapState<String, Integer> pathToQueueId;

    public QueueIdAssigner(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    @Override
    public void processElement(
            DecorateRecord record,
            ProcessFunction<DecorateRecord, DecorateRecord>.Context ctx,
            Collector<DecorateRecord> out) throws Exception {

        if (record.isDummyWatermark()){
            //broadcast to all queues
            for (int i=0; i<partitionNum; i++){
                DecorateRecord broadcastWatermark = new DecorateRecord(record.getSeqNum(), true);
                broadcastWatermark.setQueueId(i);
                out.collect(broadcastWatermark);
            }

        }
        else{

            String pathId = record.getPathInfo();
            if (pathToQueueId.contains(pathId)){
                record.setQueueId(pathToQueueId.get(pathId));
            }
            else {
                int currentSize = 0;
                for (String key: pathToQueueId.keys()){
                    currentSize++;
                }
                if (currentSize >= partitionNum){
                    for (String key: pathToQueueId.keys()){
                        LOG.warn(String.format("Existing key: %s", key));
                    }
                    LOG.warn(String.format("New key: %s", pathId));
                    throw new Exception(String.format("Number of unique path exceeds the number of allocated queues %d", partitionNum));
                }
                pathToQueueId.put(pathId, currentSize);
                record.setQueueId(currentSize);
            }
            out.collect(record);
        }
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        pathToQueueId = getRuntimeContext().getMapState(new MapStateDescriptor<>("path-to-queue-id", String.class, Integer.class));
    }
}
