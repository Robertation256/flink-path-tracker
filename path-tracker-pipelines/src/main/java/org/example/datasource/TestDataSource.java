package org.example.datasource;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class TestDataSource extends RichSourceFunction<DecorateRecord> {
    private boolean running = true;

    private boolean isInfiniteSource = true;
    private long recordsPerInvocation = 0L;
    private long seqNum = 0L;

    public TestDataSource() {
    }

    public TestDataSource(long recordsPerInvocation) {
        this.recordsPerInvocation = recordsPerInvocation;
        this.isInfiniteSource = false;
    }

    @Override
    public void run(SourceContext<DecorateRecord> sourceContext) throws Exception {
        int counter = 0;

        long recordsRemaining = this.recordsPerInvocation;
        while (isInfiniteSource || recordsRemaining > 0) {

            sourceContext.collect(
                new DecorateRecord(seqNum++, "")
            );

            if (!isInfiniteSource) {
                recordsRemaining--;
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

}
