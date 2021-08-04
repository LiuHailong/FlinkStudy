package example.processfunction;

import example.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessTest1_KeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(s -> {
            String[] split = s.split(", ");
            return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
        });

        dataStream.keyBy("id")
                        .process(new MyProcess()).print();

        env.execute();
    }

    public static class MyProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer> {

        ValueState<Long> tsTimer;

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
        }

        @Override
        public void close() throws Exception {
            tsTimer.clear();
        }

        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<Tuple, SensorReading, Integer>.Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());

            // context
            ctx.timestamp();
            ctx.getCurrentKey();
            //ctx.output();
            ctx.timerService().currentProcessingTime();
            ctx.timerService().currentWatermark();

            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000L);
            tsTimer.update(ctx.timerService().currentProcessingTime() + 1000L);
//            ctx.timerService().registerEventTimeTimer((value.getTimestamp() + 10)*1000L);
            //ctx.timerService().deleteProcessingTimeTimer();
            //ctx.timerService().deleteEventTimeTimer(tsTimer.value());
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, SensorReading, Integer>.OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + "定时触发");

        }
    }
}
