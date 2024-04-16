package org.example.operator;

import org.example.datasource.DecorateRecord;

public class TestRichMapFunctionImplForSquare extends DecorateRichMapFunction<Integer, Integer> {
    @Override
    public DecorateRecord<Integer> map(DecorateRecord<Integer> record) throws Exception {
        record.addAndSetPathInfo(instanceID);

        record.setValue(record.getValue() * record.getValue());
        return record;
    }
}
