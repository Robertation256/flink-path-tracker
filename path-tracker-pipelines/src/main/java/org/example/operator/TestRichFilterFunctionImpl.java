package org.example.operator;

import org.example.Configuration;
import org.example.datasource.DecorateRecord;

public class TestRichFilterFunctionImpl extends DecorateRichFilterFunction {
    @Override
    public boolean filter(DecorateRecord record) throws Exception {
        Thread.sleep(Configuration.OPERATOR_SLEEP_MILLIS);

        if (record.getSeqNum() % 7 == 0) {
            return false;
        }

        record.addAndSetPathInfo(instanceID);
        return true;
    }
}
