package example.transform;

import example.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest5_RighFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<String> inputStream = env.readTextFile("/Users/lhl/Downloads/work/idea_workspace/FlinkStudy/src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(s -> {
            String[] split = s.split(", ");
            return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
        });

        DataStream<Tuple2<String, Integer>> resultStream = dataStream.map(new MyMapper());

        resultStream.print();

        env.execute();
    }

    public static class MyMapper0 implements MapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), value.getId().length());
        }
    }

    public static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            RuntimeContext ctx = getRuntimeContext();
            int indexOfThisSubtask = ctx.getIndexOfThisSubtask();

            return new Tuple2<>(value.getId(), indexOfThisSubtask);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            //
            System.out.println("close");
        }
    }
}
