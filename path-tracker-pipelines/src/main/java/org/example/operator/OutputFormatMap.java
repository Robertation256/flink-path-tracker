package org.example.operator;

import org.apache.flink.api.common.functions.MapFunction;

import org.example.datasource.DecorateRecord;

// use an extra map stage to format output,
// modify the interface of the decorate function in the future
public class OutputFormatMap implements MapFunction<DecorateRecord<Integer>, String> {
    @Override
    public String map(DecorateRecord<Integer> value) throws Exception {
        return String.format("%d-%d", value.getSeqNum(), value.getValue());
    }
}
