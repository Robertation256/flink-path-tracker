package org.example.operator;

import org.apache.flink.api.common.functions.MapFunction;

import org.example.datasource.DecorateRecord;

public class DecorateRichMapFunction extends BaseDecorateRichFunction implements MapFunction<DecorateRecord, DecorateRecord> {
    @Override
    public DecorateRecord map(DecorateRecord inDecorateRecord) throws Exception {
        return null;
    }
}
