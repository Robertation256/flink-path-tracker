package org.example.pipelines;

import org.apache.flink.streaming.api.datastream.DataStream;

import org.example.datasource.DecorateRecord;
import org.example.operator.TestRichFilterFunctionImpl;
import org.example.operator.TestRichMapFunctionImplForMul2;
import org.example.operator.TestRichMapFunctionImplForSquare;

public class Workload {

    public static DataStream<DecorateRecord> attachTestPipeline(DataStream<DecorateRecord> datasource){
        return datasource.filter(new TestRichFilterFunctionImpl()).setParallelism(3)
                .rescale()
                // multiply by 2
                .map(new TestRichMapFunctionImplForMul2()).setParallelism(4)
                .keyBy(DecorateRecord::getSeqNum)
                // square it
                .map(new TestRichMapFunctionImplForSquare()).setParallelism(2);
    }
}
