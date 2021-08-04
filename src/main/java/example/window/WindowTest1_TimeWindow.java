package example.window;

import example.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class WindowTest1_TimeWindow {
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
        /*DataStream<Integer> resultStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer acc) {
                        return acc + 1;
                    }

                    @Override
                    public Integer getResult(Integer acc) {
                        return acc;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        resultStream.print();*/
        
        // 全窗口函数
        DataStream<Tuple3<String, Long, Integer>> resultStream2 = dataStream.keyBy("id").timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                        String id = tuple.getField(0).toString();
                        long end = timeWindow.getEnd();
                        int count = IteratorUtils.toList(iterable.iterator()).size();
                        collector.collect(new Tuple3<>(id, end, count));
                    }
                });
        resultStream2.print();

        //
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late"){};
        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy("id").timeWindow(Time.seconds(15)).allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag).sum("temperature");

        sumStream.getSideOutput(outputTag).print("late");
        env.execute();
    }
}
