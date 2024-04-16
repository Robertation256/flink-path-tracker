package org.example.operator;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;

public abstract class BaseDecorateRichFunction extends AbstractRichFunction {
    String instanceID;

    @Override
    public void open(Configuration config) {
        int subID = getRuntimeContext().getIndexOfThisSubtask();
        String operatorName = getRuntimeContext().getTaskName();
        instanceID = String.format("%s_%d", operatorName, subID);
    }
}
