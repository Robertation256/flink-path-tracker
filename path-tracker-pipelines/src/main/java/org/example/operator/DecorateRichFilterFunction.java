package org.example.operator;

import org.apache.flink.api.common.functions.FilterFunction;

import org.example.datasource.DecorateRecord;

public class DecorateRichFilterFunction<IN> extends BaseDecorateRichFunction implements FilterFunction<DecorateRecord<IN>> {
    @Override
    public boolean filter(DecorateRecord<IN> inDecorateRecord) throws Exception {
        return false;
    }
}
