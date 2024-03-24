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

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class Main {
    public static void main(String[] args) throws  Exception{


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());

        env.addSource(new TestDataSource(100)).setParallelism(1)
                // filter out multiples of 7
                .filter(new TestRichFilterFunctionImpl()).setParallelism(3)
                .rescale()
                // multiply by 2
                .map(new TestRichMapFunctionImplForMul2()).setParallelism(4)
                .keyBy(new KeySelector<DecorateRecord<Integer>, Object>() {
                    @Override
                    public Object getKey(DecorateRecord<Integer> record) throws Exception {
                        return record.getValue();
                    }
                })
                // square it
                .map(new TestRichMapFunctionImplForSquare()).setParallelism(2)
                .print();

        env.execute();
    }
}

class DecorateRecord<T> {
    private long seqNum;
    private String pathInfo;

    private T value;

    public DecorateRecord(long SeqNum, String pathInfo, T value) {
        this.seqNum = SeqNum;
        this.pathInfo = pathInfo;
        this.value = value;
    }

    public void setSeqNum(long seqNum) {
        this.seqNum = seqNum;
    }

    public long getSeqNum() {
        return seqNum;
    }

    // TODO: use xor to compress path information?
    public void addAndSetPathInfo(String vertexID) {
        this.pathInfo = String.format("%s-%s", this.pathInfo, vertexID);
    }

    public String addPathInfo(String vertexID) {
        return String.format("%s-%s", this.pathInfo, vertexID);
    }

    public void setPathInfo(String pathInfo) {
        this.pathInfo = pathInfo;
    }

    public String getPathInfo() {
        return this.pathInfo;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.format("{SeqNumber=%d, PathInfo=(%s), Value=%s}", this.getSeqNum(), this.getPathInfo(), this.getValue());
    }
}

class SeqNumInjectionFunction<T> extends ProcessFunction<T, DecorateRecord<T>> {
    private long SeqNum;

    @Override
    public void open(Configuration config) {
        this.SeqNum = 0L;
    }

    @Override
    public void processElement(T value, ProcessFunction<T, DecorateRecord<T>>.Context context, Collector<DecorateRecord<T>> collector) throws Exception {
        SeqNum++;
        collector.collect(new DecorateRecord<>(SeqNum, "", value));
    }
}

abstract class BaseDecorateRichFunction extends AbstractRichFunction {
    String instanceID;
    @Override
    public void open(Configuration config) {
        int subID = getRuntimeContext().getIndexOfThisSubtask();
        String operatorName = getRuntimeContext().getTaskName();
        instanceID = String.format("%s_%d", operatorName, subID);
    }
}

class DecorateRichFlatMapFunction<IN, OUT> extends BaseDecorateRichFunction implements FlatMapFunction<DecorateRecord<IN>, DecorateRecord<OUT>> {
    @Override
    public void flatMap(DecorateRecord<IN> record, Collector<DecorateRecord<OUT>> collector) throws Exception {

    }
}

class DecorateRichFilterFunction<IN> extends BaseDecorateRichFunction implements FilterFunction<DecorateRecord<IN>> {
    @Override
    public boolean filter(DecorateRecord<IN> inDecorateRecord) throws Exception {
        return false;
    }
}

class DecorateRichMapFunction<IN, OUT> extends BaseDecorateRichFunction implements MapFunction<DecorateRecord<IN>, DecorateRecord<OUT>> {
    @Override
    public DecorateRecord<OUT> map(DecorateRecord<IN> inDecorateRecord) throws Exception {
        return null;
    }
}

class TestDataSource extends RichSourceFunction<DecorateRecord<Integer>> {
    private boolean running = true;

    private boolean isInfiniteSource = true;
    private long recordsPerInvocation = 0L;
    private long SeqNum;

    public TestDataSource(){}

    public TestDataSource(long recordsPerInvocation){
        this.recordsPerInvocation = recordsPerInvocation;
        this.isInfiniteSource = false;
    }

    @Override
    public void open(Configuration config) {
        this.SeqNum = 0L;
    }

    @Override
    public void run(SourceContext<DecorateRecord<Integer>> sourceContext) throws Exception {
        int counter = 0;

        long recordsRemaining = this.recordsPerInvocation;
        while(isInfiniteSource || recordsRemaining > 0){

            sourceContext.collect(new DecorateRecord<Integer>(SeqNum++, "", counter++));

            if (!isInfiniteSource){
                recordsRemaining--;
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}

class TestRichFilterFunctionImpl extends DecorateRichFilterFunction<Integer> {
    @Override
    public boolean filter(DecorateRecord<Integer> record) throws Exception {
        if (record.getValue() % 7 == 0 ) {
            return false;
        }

        record.addAndSetPathInfo(instanceID);
        return true;
    }
}

class TestRichMapFunctionImplForMul2 extends DecorateRichMapFunction<Integer, Integer> {
    @Override
    public DecorateRecord<Integer> map(DecorateRecord<Integer> record) throws Exception {
        record.addAndSetPathInfo(instanceID);

        record.setValue(record.getValue() * 2);
        return record;
    }
}

class TestRichMapFunctionImplForSquare extends DecorateRichMapFunction<Integer, Integer> {
    @Override
    public DecorateRecord<Integer> map(DecorateRecord<Integer> record) throws Exception {
        record.addAndSetPathInfo(instanceID);

        record.setValue(record.getValue() * record.getValue());
        return record;
    }
}
