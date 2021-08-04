package example.state;

import example.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StateTest3_KeyedStateApplicationCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(s -> {
            String[] split = s.split(", ");
            return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
        });

        // 定义一个有状态的map操作, 统计当前sensor数据个数
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> resultStream = dataStream.keyBy("id")
                        .flatMap(new TempChangeWarning(10.0));

        resultStream.print();
        env.execute();
    }


    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {

        private Double threshold;

        public TempChangeWarning(double threshold) {
            this.threshold = threshold;
        }

        // 定义状态 保存上一次的温度值
        private ValueState<Double> lastTempState;
        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("my-value", Double.class));
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTemp = lastTempState.value();
            if(null != lastTemp) {
                Double curTemp = sensorReading.getTemperature();
                if(Math.abs(curTemp - lastTemp) >= threshold) {
                    out.collect(new Tuple3<String, Double, Double>(sensorReading.getId(), lastTemp, curTemp));
                }
            }
            lastTempState.update(sensorReading.getTemperature());
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
