package org.example.operator;

import org.example.datasource.DecorateRecord;

public class TestRichFilterFunctionImpl extends DecorateRichFilterFunction<Integer> {
    @Override
    public boolean filter(DecorateRecord<Integer> record) throws Exception {
        if (record.getValue() % 7 == 0) {
            return false;
        }

        record.addAndSetPathInfo(instanceID);
        return true;
    }
}
