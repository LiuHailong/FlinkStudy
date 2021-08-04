package example.window;

import example.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowTest2_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

//        DataStream<String> inputStream = env.readTextFile("/Users/lhl/Downloads/work/idea_workspace/FlinkStudy/src/main/resources/sensor.txt");

        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(s -> {
            String[] split = s.split(", ");
            return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
        });

        // 增量聚合函数
        DataStream<Double> resultStream = dataStream.keyBy("id")
                .countWindow(10, 2)
                .aggregate(new MyAvgFunction());

        resultStream.print();
        
        // 全窗口函数
        /*DataStream<Tuple3<String, Long, Integer>> resultStream2 = dataStream.keyBy("id").timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                        String id = tuple.getField(0).toString();
                        long end = timeWindow.getEnd();
                        int count = IteratorUtils.toList(iterable.iterator()).size();
                        collector.collect(new Tuple3<>(id, end, count));
                    }
                });
        resultStream2.print();*/
        env.execute();
    }

    public static class MyAvgFunction implements AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double> {

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading sensorReading, Tuple2<Double, Integer> t) {
            return new Tuple2<>(t.f0 + sensorReading.getTemperature(), t.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> t) {
            return t.f0 / t.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> t, Tuple2<Double, Integer> acc) {
            return new Tuple2<>(t.f0 + acc.f0, t.f1 + acc.f1);
        }
    }
}
