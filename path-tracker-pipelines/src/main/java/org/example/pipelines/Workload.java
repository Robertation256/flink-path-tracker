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

package org.example.pipelines;

import org.apache.flink.streaming.api.datastream.DataStream;

import org.example.datasource.DecorateRecord;
import org.example.operator.TestRichFilterFunctionImpl;
import org.example.operator.TestRichMapFunction;

import java.time.Instant;

public class Workload {

    public static DataStream<DecorateRecord> attachTestPipeline(DataStream<DecorateRecord> datasource){
        return datasource
                .filter(new TestRichFilterFunctionImpl()).setParallelism(15)
                .forward()
                // multiply by 2
                .map(new TestRichMapFunction()).setParallelism(15)
                .rebalance()
                // square it
                .map(new TestRichMapFunction()).setParallelism(2)
                .forward()
                .map(
                    record -> {
                        record.setProcessCompletionTime(Instant.now().toEpochMilli());
                        return record;
                    }).setParallelism(2);
    }
}
