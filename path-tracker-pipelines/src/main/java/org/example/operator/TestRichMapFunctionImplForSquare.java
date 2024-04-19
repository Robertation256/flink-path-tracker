package org.example.operator;

import org.example.Configuration;
import org.example.datasource.DecorateRecord;

public class TestRichMapFunctionImplForSquare extends DecorateRichMapFunction {
    @Override
    public DecorateRecord map(DecorateRecord record) throws Exception {
        record.addAndSetPathInfo(instanceID);
        Thread.sleep(Configuration.OPERATOR_SLEEP_MILLIS);
        // todo: either insert real workload or reduce all operators to dummy workload with induced sleep
        return record;
    }
}
