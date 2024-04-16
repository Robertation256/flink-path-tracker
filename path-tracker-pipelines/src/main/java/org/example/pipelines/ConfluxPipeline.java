package org.example.pipelines;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.example.CustomKafkaSerializer;
import org.example.datasource.DecorateRecord;
import org.example.datasource.TestDataSource;
import org.example.operator.TestRichFilterFunctionImpl;
import org.example.operator.TestRichMapFunctionImplForMul2;
import org.example.operator.TestRichMapFunctionImplForSquare;

import java.util.Properties;

public class ConfluxPipeline {

    public static StreamExecutionEnvironment create(String kafkaBootstrapServer, String outputTopic){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.example.CustomKafkaPartitioner");

        KafkaSink<DecorateRecord<Integer>> kafkaSink = KafkaSink.<DecorateRecord<Integer>>builder()
                .setBootstrapServers(kafkaBootstrapServer)
                .setRecordSerializer(new CustomKafkaSerializer(outputTopic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(producerProps)
                .build();

        env.addSource(new TestDataSource(100000)).setParallelism(1)
                // filter out multiples of 7
                .filter(new TestRichFilterFunctionImpl()).setParallelism(3)
                .rescale()
                // multiply by 2
                .map(new TestRichMapFunctionImplForMul2()).setParallelism(4)
                .keyBy(new KeySelector<DecorateRecord<Integer>, Object>() {
                    @Override
                    public Object getKey(DecorateRecord<Integer> record) throws Exception {
                        return record.getValue();
                    }
                })
                // square it
                .map(new TestRichMapFunctionImplForSquare()).setParallelism(2)
                .sinkTo(kafkaSink).setParallelism(1);


        return env;
    }
}
