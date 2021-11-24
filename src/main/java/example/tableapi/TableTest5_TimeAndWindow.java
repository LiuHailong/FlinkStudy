package example.tableapi;


import example.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author liohalo
 * @version 3.0.0
 * @ClassName Example.java
 * @Description TODO
 * @createTime 2021/11/3
 */
public class TableTest5_TimeAndWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<String> inputStream = env.readTextFile("/Users/lhl/Downloads/work/idea_workspace/FlinkStudy/src/main/resources/sensor1.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading sensorReading) {
                return sensorReading.getTimestamp() * 1000L;
            }
        });

        // 窗口操作
        // Group Window
        // table API
        Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts,  temperature as temp, rt.rowtime");

        tableEnv.createTemporaryView("sensor", dataTable);

        Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))
                .groupBy("id, tw")
                .select("id, id.count, temp.avg, tw.end");

        // SQL
        Table resultSqlTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp, tumble_end(rt, interval '10' second) " +
                " from sensor group by id, tumble(rt, interval '10' second)");

        // Over Window
        // table API
        Table overResult = dataTable.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"))
                .select("id, id.count over ow, temp.avg over ow");

        // SQL
        Table overSqlResult = tableEnv.sqlQuery("select id, count(id) over ow, avg(temp) over ow" +
                " from sensor" +
                " window ow as (PARTITION BY id ORDER BY rt ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)");


//        dataTable.printSchema();
/*        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");*/
        tableEnv.toAppendStream(overResult, Row.class).print("overResult");
        tableEnv.toAppendStream(overSqlResult, Row.class).print("overSqlResult");

        env.execute();
    }
}
