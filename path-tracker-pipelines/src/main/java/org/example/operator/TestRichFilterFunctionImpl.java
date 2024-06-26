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

import org.apache.flink.api.common.functions.AbstractRichFunction;

import org.apache.flink.api.common.functions.FilterFunction;

import org.example.Configuration;
import org.example.datasource.DecorateRecord;

public class TestRichFilterFunctionImpl extends AbstractRichFunction implements FilterFunction<DecorateRecord> {
    @Override
    public boolean filter(DecorateRecord record) {
        long cycles = Configuration.OPERATOR_WORKLOAD_CYCLES;
        while (cycles > 0){
            cycles--;
        }

        record.addAndSetPathInfo(getRuntimeContext().getTaskInfo().getTaskName(), getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());

        return record.getSeqNum() % 7 != 0;
    }
}
