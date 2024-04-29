package org.example.operator;

import org.apache.flink.api.common.functions.AbstractRichFunction;

import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;

import org.example.Configuration;
import org.example.datasource.DecorateRecord;

public class TestRichMapFunction extends AbstractRichFunction implements MapFunction<DecorateRecord, DecorateRecord> {

    @Override
    public DecorateRecord map(DecorateRecord record) {
        long cycles = Configuration.OPERATOR_WORKLOAD_CYCLES;
        while (cycles > 0){
            cycles--;
        }

        record.addAndSetPathInfo(getRuntimeContext().getTaskInfo().getTaskName(), getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        return record;
    }



}
