package example.state;

import example.beans.SensorReading;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTest4_FaultTolerance {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 状态后端设置
        env.setStateBackend(new MemoryStateBackend());

        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(s -> {
            String[] split = s.split(", ");
            return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
        });


        dataStream.print();
        env.execute();
    }


}
