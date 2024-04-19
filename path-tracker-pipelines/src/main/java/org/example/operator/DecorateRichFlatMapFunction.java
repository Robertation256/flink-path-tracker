package org.example.operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import org.example.datasource.DecorateRecord;

public class DecorateRichFlatMapFunction extends BaseDecorateRichFunction implements FlatMapFunction<DecorateRecord, DecorateRecord> {
    @Override
    public void flatMap(
            DecorateRecord record,
            Collector<DecorateRecord> collector) throws Exception {

    }
}
