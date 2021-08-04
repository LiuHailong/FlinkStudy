package example.transform;

import example.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputStream = env.readTextFile("/Users/lhl/Downloads/work/idea_workspace/FlinkStudy/src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(s -> {
            String[] split = s.split(", ");
            return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
        });

        // 分组
//        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(SensorReading::getId);
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        // 滚动聚合
        DataStream<SensorReading> resultStream = keyedStream.maxBy("temperature");

        resultStream.print();

        env.execute();
    }
}
