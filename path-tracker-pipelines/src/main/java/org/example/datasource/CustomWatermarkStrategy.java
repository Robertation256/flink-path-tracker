package org.example.datasource;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;


public class CustomWatermarkStrategy<T> implements WatermarkStrategy<DecorateRecord> {

    @Override
    public WatermarkGenerator<DecorateRecord> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<DecorateRecord>() {
            private long maxSequenceNumberSeen = Long.MIN_VALUE;

            @Override
            public void onEvent(
                    DecorateRecord event,
                    long eventTimestamp,
                    WatermarkOutput output) {
                maxSequenceNumberSeen = Math.max(maxSequenceNumberSeen, event.getSeqNum());
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {
                output.emitWatermark(new Watermark(maxSequenceNumberSeen));
            }
        };
    }

    @Override
    public TimestampAssigner<DecorateRecord> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return (event, recordTimestamp) -> event.getSeqNum();
    }
}

