package org.example.operator;

import org.example.datasource.DecorateRecord;

public class TestRichMapFunctionImplForMul2 extends DecorateRichMapFunction<Integer, Integer> {
    @Override
    public DecorateRecord<Integer> map(DecorateRecord<Integer> record) throws Exception {
        record.addAndSetPathInfo(instanceID);

        record.setValue(record.getValue() * 2);
        return record;
    }
}
