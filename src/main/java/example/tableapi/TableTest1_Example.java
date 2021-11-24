package example.tableapi;

import example.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author liohalo
 * @version 3.0.0
 * @ClassName Example.java
 * @Description TODO
 * @createTime 2021/11/3
 */
public class TableTest1_Example {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.readTextFile("/Users/lhl/Downloads/work/idea_workspace/FlinkStudy/src/main/resources/sensor.txt");
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // table api
        // 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    }
}
