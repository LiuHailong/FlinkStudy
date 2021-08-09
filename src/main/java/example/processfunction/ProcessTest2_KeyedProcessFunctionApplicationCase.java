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

/**
 * 温度值在10s内连续上升, 则报警
 */
public class ProcessTest2_KeyedProcessFunctionApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(s -> {
            String[] split = s.split(", ");
            return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
        });

        dataStream.keyBy("id").process(new WarningFunction(10)).print();

        env.execute();
    }

    public static class WarningFunction extends KeyedProcessFunction<Tuple, SensorReading, String> {
        private Integer interval;

        // 上一次温度
        private ValueState<Double> lastTemp;
        // 上一个定时器
        private ValueState<Long> lastTimer;

        public WarningFunction(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class, Double.MIN_VALUE));
            lastTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("last-timer", Long.class));
        }

        @Override
        public void processElement(SensorReading sensorReading, KeyedProcessFunction<Tuple, SensorReading, String>.Context ctx, Collector<String> collector) throws Exception {
            Long ts = lastTimer.value();
            // 如果温度大于上一个温度, 且定时器为空, 注册定时器
            if (sensorReading.getTemperature() > lastTemp.value() && ts == null) {
                ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                lastTimer.update(ts);
            }
            // 如果温度小于上一个温度, 且定时器不为空, 删除定时器
            else if (sensorReading.getTemperature() < lastTemp.value() && ts != null) {
                ctx.timerService().deleteProcessingTimeTimer(ts);
                lastTimer.clear();
            }

            // 更新状态
            lastTemp.update(lastTemp.value());
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("传感器: "+ctx.getCurrentKey() + " 10s内当前的温度连续上升");
            lastTimer.clear();
        }

        @Override
        public void close() throws Exception {
            lastTemp.clear();
        }
    }
}
