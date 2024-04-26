/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example.pipelines;


import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.example.CustomKafkaSerializer;
import org.example.datasource.CustomWatermarkStrategy;
import org.example.datasource.DecorateRecord;
import org.example.datasource.TestDataSource;
import org.example.operator.TestRichFilterFunctionImpl;
import org.example.operator.TestRichMapFunctionImplForMul2;
import org.example.operator.TestRichMapFunctionImplForSquare;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Properties;


public class GlobalSortPipeline {

    public static StreamExecutionEnvironment create(String kafkaServers, String outputTopic){
        long windowSize = 1000L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();
        config.set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ofMillis(org.example.Configuration.WATERMARK_EMISSION_PERIOD_MILLIS));
        env.configure(config);


        DataStream<DecorateRecord> source = env.addSource(new TestDataSource(org.example.Configuration.DATASOURCE_SIZE)).setParallelism(1)

                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy<>());


        DataStream<DecorateRecord> records = Workload.attachTestPipeline(source);
        DataStream<DecorateRecord> sorted = records.keyBy(t -> 1)
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(windowSize)))
                .process(getWindowFunction()).setParallelism(1);


        sorted.sinkTo(getRecordSink(kafkaServers, outputTopic)).setParallelism(1);

        sorted.keyBy(t -> 1).
                process(getCheckerFunction()).setParallelism(1)
                .sinkTo(new DiscardingSink<>());

        return env;
    }

    private static KafkaSink<DecorateRecord> getRecordSink(String kafkaServer, String topic){


        return KafkaSink.<DecorateRecord>builder()
                .setBootstrapServers(kafkaServer)
                .setRecordSerializer(new CustomKafkaSerializer(topic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

    }



    private static WatermarkStrategy<DecorateRecord> getWatermarkStrategy(){
        return new WatermarkStrategy<DecorateRecord>() {
            @Override
            public WatermarkGenerator<DecorateRecord> createWatermarkGenerator(
                    WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<DecorateRecord>() {
                    private long currentSeqNum = 0L;
                    @Override
                    public void onEvent(
                            DecorateRecord event,
                            long eventTimestamp,
                            WatermarkOutput output) {
                        currentSeqNum = Math.max(event.getSeqNum(), currentSeqNum);

                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(currentSeqNum));

                    }
                };
            }

            @Override public TimestampAssigner<DecorateRecord> createTimestampAssigner(
                    TimestampAssignerSupplier.Context context) {

                return ((element, recordTimestamp) -> element.getSeqNum());
            }
        };
    }




    private static ProcessFunction<DecorateRecord, DecorateRecord> getCheckerFunction(){
        return new ProcessFunction<DecorateRecord, DecorateRecord>(){

            private ValueState<Long> prevSeqNum;
            @Override
            public void open(OpenContext openContext) throws Exception {
                prevSeqNum = getRuntimeContext().getState(new ValueStateDescriptor<>("prev-seq-num", Long.class));
            }

            @Override
            public void processElement(
                    DecorateRecord value,
                    ProcessFunction<DecorateRecord, DecorateRecord>.Context ctx,
                    Collector<DecorateRecord> out) throws Exception {
                if (prevSeqNum.value() == null){
                    prevSeqNum.update(-1L);
                }

                if (value.getSeqNum() <= prevSeqNum.value()){
                    throw new Exception("Order check failed");
                }

                prevSeqNum.update(value.getSeqNum());
                out.collect(value);

            }
        };
    }


    private static ProcessWindowFunction<DecorateRecord, DecorateRecord, Integer, TimeWindow> getWindowFunction(){
        return new ProcessWindowFunction<DecorateRecord, DecorateRecord, Integer, TimeWindow>(){
            @Override
            public void process(
                    Integer integer,
                    ProcessWindowFunction<DecorateRecord, DecorateRecord, Integer, TimeWindow>.Context context,
                    Iterable<DecorateRecord> elements,
                    Collector<DecorateRecord> out) throws Exception {

                ArrayList<DecorateRecord> buffer = new ArrayList<>();
                for(DecorateRecord element: elements){
                    buffer.add(element);
                }

                buffer.sort((o1, o2) -> {
                    long diff = o1.getSeqNum() - o2.getSeqNum();
                    if (diff == 0) {
                        return 0;
                    } else if (diff > 0) {
                        return 1;
                    }
                    return -1;
                });

                for (DecorateRecord record: buffer){
                    record.setSinkTime(Instant.now().toEpochMilli());
                    out.collect(record);
                }
            }
        };
    }
}
