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

import java.util.ArrayList;
import java.util.List;

// access and emit watermarks
public class CustomWatermarkProcessor extends AbstractStreamOperator<DecorateRecord> implements OneInputStreamOperator<DecorateRecord, DecorateRecord> {
    private String taskName = null;
    private Integer subTaskId = null;
    private List<String> pathIds = new ArrayList<>();
    private List<String> broadcastTargets = new ArrayList<>();

    public void setPathIds(List<String> pathIds){
        this.pathIds = pathIds;
    }


    @Override
    public void processElement(StreamRecord<DecorateRecord> element) {
        if (taskName == null || subTaskId == null){
            taskName = getRuntimeContext().getTaskInfo().getTaskName();
            subTaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        }
        element.getValue().addAndSetPathInfo(taskName, subTaskId);
        output.collect(element);
    }

    @Override
    public void processWatermark(org.apache.flink.streaming.api.watermark.Watermark mark) {
        if (broadcastTargets.isEmpty()){
            String vertexId = getRuntimeContext().getTaskInfo().getTaskName() + "_" + getRuntimeContext()
                    .getTaskInfo()
                    .getIndexOfThisSubtask();
            for (String pathId : pathIds){
                if (pathId.contains(vertexId)){
                    broadcastTargets.add(pathId);
                }
            }
        }

        for (String pathId : broadcastTargets){
            DecorateRecord watermarkRecord = new DecorateRecord(mark.getTimestamp(), true);
            watermarkRecord.setPathInfo(pathId);
            output.collect(new StreamRecord<>(watermarkRecord));
        }

        output.emitWatermark(mark);
    }
}
