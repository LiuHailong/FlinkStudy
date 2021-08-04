package example.sink;

import example.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("/Users/lhl/Downloads/work/idea_workspace/FlinkStudy/src/main/resources/sensor.txt");

        DataStream<String> dataStream = inputStream.map(s -> {
            String[] split = s.split(", ");
            return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2])).toString();
        });

        dataStream.addSink(new FlinkKafkaProducer011<String>("hadoopcm1:9092", "sinktest", new SimpleStringSchema()));

        env.execute();
    }
}
