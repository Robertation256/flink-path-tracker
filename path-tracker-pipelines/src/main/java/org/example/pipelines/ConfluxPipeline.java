package org.example.pipelines;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.PathAnalyzer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.example.CustomKafkaSerializer;
import org.example.datasource.CustomWatermarkStrategy;
import org.example.datasource.DecorateRecord;
import org.example.datasource.TestDataSource;
import org.example.operator.CustomWatermarkProcessor;
import org.example.operator.QueueIdAssigner;
import org.example.utils.KafkaAdminUtils;

import java.util.Properties;

public class ConfluxPipeline {

    private static int pathNum = -1;

    public static StreamExecutionEnvironment create(String kafkaBootstrapServer, String recordOutputTopic) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<DecorateRecord> customWatermarkStrategy = new CustomWatermarkStrategy<>();

        DataStream<DecorateRecord> datasource = env
                .addSource(new TestDataSource(1000)).setParallelism(1)
                .assignTimestampsAndWatermarks(customWatermarkStrategy).setParallelism(1);
        DataStream<DecorateRecord> recordStream = Workload.attachTestPipeline(datasource);

        // checking path num util this point is good enough since the later operators have parallelism=1
        pathNum = PathAnalyzer.computePathNum(env);


        recordStream
                .transform("watermark-extraction",
                        TypeInformation.of(new TypeHint<DecorateRecord>() {}),
                        new CustomWatermarkProcessor())
                .setParallelism(1)
                .keyBy(r -> 1)
                .process(new QueueIdAssigner(pathNum)).setParallelism(1)
                .sinkTo(getRecordSink(kafkaBootstrapServer, recordOutputTopic)).setParallelism(1);



        return env;
    }

    public static int getPathNum() throws  Exception{
        if (pathNum < 0){
            create("", "");
        }
        return pathNum;
    }

    private static KafkaSink<DecorateRecord> getRecordSink(String kafkaServer, String topic){
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.example.utils.CustomKafkaPartitioner");

        return KafkaSink.<DecorateRecord>builder()
                .setBootstrapServers(kafkaServer)
                .setRecordSerializer(new CustomKafkaSerializer(topic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(producerProps)
                .build();

    }


    public static void main(String[] args) throws  Exception{
        create("", "");
    }
}
