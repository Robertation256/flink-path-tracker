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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.datasource.DecorateRecord;
import org.example.datasource.TestDataSource;
import org.example.operator.TestRichFilterFunctionImpl;
import org.example.operator.TestRichMapFunctionImplForMul2;
import org.example.operator.TestRichMapFunctionImplForSquare;
import java.time.Duration;
import java.util.ArrayList;


public class GlobalSortPipeline {

    public static StreamExecutionEnvironment create(){
        long windowSize = 1000L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.addSource(new TestDataSource(100000)).setParallelism(1)
                // filter out multiples of 7
                .assignTimestampsAndWatermarks(getWatermarkStrategy())
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
                .keyBy(t -> 1)
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(windowSize)))
                .process(getWindowFunction()).setParallelism(1)
                .keyBy(t -> 1).
                process(getCheckerFunction()).setParallelism(1)
                .print().setParallelism(1);


        return env;
    }



    private static WatermarkStrategy<DecorateRecord<Integer>> getWatermarkStrategy(){
        return new WatermarkStrategy<DecorateRecord<Integer>>() {
            @Override
            public WatermarkGenerator<DecorateRecord<Integer>> createWatermarkGenerator(
                    WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<DecorateRecord<Integer>>() {
                    private long currentSeqNum = 0L;
                    @Override
                    public void onEvent(
                            DecorateRecord<Integer> event,
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

            @Override public TimestampAssigner<DecorateRecord<Integer>> createTimestampAssigner(
                    TimestampAssignerSupplier.Context context) {

                return ((element, recordTimestamp) -> element.getSeqNum());
            }
        };
    }

    private static ProcessFunction<DecorateRecord<Integer>, DecorateRecord<Integer>> getCheckerFunction(){
        return new ProcessFunction<DecorateRecord<Integer>, DecorateRecord<Integer>>(){

            private ValueState<Long> prevSeqNum;
            @Override
            public void open(OpenContext openContext) throws Exception {
                prevSeqNum = getRuntimeContext().getState(new ValueStateDescriptor<>("prev-seq-num", Long.class));
            }

            @Override
            public void processElement(
                    DecorateRecord<Integer> value,
                    ProcessFunction<DecorateRecord<Integer>, DecorateRecord<Integer>>.Context ctx,
                    Collector<DecorateRecord<Integer>> out) throws Exception {
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


    private static ProcessWindowFunction<DecorateRecord<Integer>, DecorateRecord<Integer>, Integer, TimeWindow> getWindowFunction(){
        return new ProcessWindowFunction<DecorateRecord<Integer>, DecorateRecord<Integer>, Integer, TimeWindow>(){
            @Override
            public void process(
                    Integer integer,
                    ProcessWindowFunction<DecorateRecord<Integer>, DecorateRecord<Integer>, Integer, TimeWindow>.Context context,
                    Iterable<DecorateRecord<Integer>> elements,
                    Collector<DecorateRecord<Integer>> out) throws Exception {

                ArrayList<DecorateRecord<Integer>> buffer = new ArrayList<>();
                for(DecorateRecord<Integer> element: elements){
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

                for (DecorateRecord<Integer> record: buffer){
                    out.collect(record);
                }
            }
        };
    }
}
