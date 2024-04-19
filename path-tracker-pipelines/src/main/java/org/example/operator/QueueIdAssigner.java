package org.example.operator;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import org.apache.flink.util.Collector;

import org.example.datasource.DecorateRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueIdAssigner extends ProcessFunction<DecorateRecord, DecorateRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(QueueIdAssigner.class);

    private final int partitionNum;

    private MapState<String, Integer> pathToQueueId;

    public QueueIdAssigner(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    @Override
    public void processElement(
            DecorateRecord record,
            ProcessFunction<DecorateRecord, DecorateRecord>.Context ctx,
            Collector<DecorateRecord> out) throws Exception {

        if (record.isDummyWatermark()){
            //broadcast to all queues
            for (int i=0; i<partitionNum; i++){
                DecorateRecord broadcastWatermark = new DecorateRecord(record.getSeqNum(), true);
                broadcastWatermark.setQueueId(i);
                out.collect(broadcastWatermark);
            }

        }
        else{

            String pathId = record.getPathInfo();
            if (pathToQueueId.contains(pathId)){
                record.setQueueId(pathToQueueId.get(pathId));
            }
            else {
                int currentSize = 0;
                for (String key: pathToQueueId.keys()){
                    currentSize++;
                }
                if (currentSize >= partitionNum){
                    for (String key: pathToQueueId.keys()){
                        LOG.warn(String.format("Existing key: %s", key));
                    }
                    LOG.warn(String.format("New key: %s", pathId));
                    throw new Exception(String.format("Number of unique path exceeds the number of allocated queues %d", partitionNum));
                }
                pathToQueueId.put(pathId, currentSize);
                record.setQueueId(currentSize);
            }
            out.collect(record);
        }
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        pathToQueueId = getRuntimeContext().getMapState(new MapStateDescriptor<>("path-to-queue-id", String.class, Integer.class));
    }
}
