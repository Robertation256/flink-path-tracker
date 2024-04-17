package org.example.pipelines;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.OutputTag;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.example.CustomKafkaSerializer;
import org.example.WatermarkKafkaSerializer;
import org.example.datasource.CustomWatermarkStrategy;
import org.example.datasource.DecorateRecord;
import org.example.datasource.TestDataSource;
import org.example.operator.CustomWatermarkProcessor;
import org.example.operator.TestRichFilterFunctionImpl;
import org.example.operator.TestRichMapFunctionImplForMul2;
import org.example.operator.TestRichMapFunctionImplForSquare;

import java.util.Properties;

public class ConfluxPipeline {

    public static StreamExecutionEnvironment create(String kafkaBootstrapServer, String recordOutputTopic, String watermarkOutputTopic){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.example.CustomKafkaPartitioner");

        KafkaSink<DecorateRecord<Integer>> kafkaSink = KafkaSink.<DecorateRecord<Integer>>builder()
                .setBootstrapServers(kafkaBootstrapServer)
                .setRecordSerializer(new CustomKafkaSerializer(recordOutputTopic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(producerProps)
                .build();

        WatermarkStrategy<DecorateRecord<Integer>> customWatermarkStrategy = new CustomWatermarkStrategy<>();

        DataStream<DecorateRecord<Integer>> recordStream = env.addSource(new TestDataSource(100000)).setParallelism(1)
                // apply watermark strategy
                .assignTimestampsAndWatermarks(customWatermarkStrategy)
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
                .map(new TestRichMapFunctionImplForSquare()).setParallelism(2);

        recordStream.sinkTo(kafkaSink).setParallelism(1);

        // init side output stream for watermarks
        OutputTag<DecorateRecord<Integer>> watermarkRecordTag = new OutputTag<DecorateRecord<Integer>>("watermark"){};
        DataStream<DecorateRecord<Integer>> watermarkSideOutputStream = recordStream.transform("Watermark Extractor", TypeInformation.of(new TypeHint<DecorateRecord<Integer>>() {
        }), new CustomWatermarkProcessor<>(watermarkRecordTag)).setParallelism(1).getSideOutput(watermarkRecordTag);

        // TODO: confirm whether this init is right
        Properties watermarkProducerProps = new Properties();
        KafkaSink<DecorateRecord<Integer>> watermarkKafkaSink = KafkaSink.<DecorateRecord<Integer>>builder()
                .setBootstrapServers(kafkaBootstrapServer)
                .setRecordSerializer(new WatermarkKafkaSerializer<Integer>(watermarkOutputTopic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(watermarkProducerProps)
                .build();
        watermarkSideOutputStream.sinkTo(watermarkKafkaSink).setParallelism(1);

        return env;
    }
}
