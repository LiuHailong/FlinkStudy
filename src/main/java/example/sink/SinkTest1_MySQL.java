package example.sink;

import example.beans.SensorReading;
import example.source.SourceTest4_UDF;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkTest1_MySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*DataStream<String> inputStream = env.readTextFile("/Users/lhl/Downloads/work/idea_workspace/FlinkStudy/src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(s -> {
            String[] split = s.split(", ");
            return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
        });*/

        DataStream<SensorReading> dataStream = env.addSource(new SourceTest4_UDF.MySensorSource());

        dataStream.addSink(new MyJDBCFunction());

        env.execute();
    }

    public static class MyJDBCFunction extends RichSinkFunction<SensorReading> {

        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://192.168.70.82:3306/bi", "root", "ody,123");
            insertStmt = connection.prepareStatement("insert into temp (id, temp) values(?, ?)");
            updateStmt = connection.prepareStatement("update temp set temp = ? where id = ?");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            updateStmt.setDouble(1, value.getTemperature());
            updateStmt.setString(2, value.getId());
            updateStmt.execute();
            if(updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getTemperature());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }
}
