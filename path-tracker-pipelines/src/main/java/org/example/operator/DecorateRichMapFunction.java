package org.example.operator;

import org.apache.flink.api.common.functions.MapFunction;

import org.example.datasource.DecorateRecord;

public class DecorateRichMapFunction<IN, OUT> extends BaseDecorateRichFunction implements MapFunction<DecorateRecord<IN>, DecorateRecord<OUT>> {
    @Override
    public DecorateRecord<OUT> map(DecorateRecord<IN> inDecorateRecord) throws Exception {
        return null;
    }
}
