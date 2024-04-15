package org.example.operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import org.example.datasource.DecorateRecord;

public class DecorateRichFlatMapFunction<IN, OUT> extends BaseDecorateRichFunction implements FlatMapFunction<DecorateRecord<IN>, DecorateRecord<OUT>> {
    @Override
    public void flatMap(
            DecorateRecord<IN> record,
            Collector<DecorateRecord<OUT>> collector) throws Exception {

    }
}
