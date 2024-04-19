package org.example.operator;

import org.apache.flink.api.common.functions.FilterFunction;

import org.example.datasource.DecorateRecord;

public class DecorateRichFilterFunction extends BaseDecorateRichFunction implements FilterFunction<DecorateRecord> {
    @Override
    public boolean filter(DecorateRecord inDecorateRecord) throws Exception {
        return false;
    }
}
