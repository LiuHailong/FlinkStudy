package example.tableapi;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author liohalo
 * @version 3.0.0
 * @ClassName Example.java
 * @Description TODO
 * @createTime 2021/11/3
 */
public class TableTest4_KafkaPipeline {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 连接kafka读取数据
        tableEnv.connect(new Kafka()
                        .version("0.11")
                        .topic("sensor")
                        .property("zookeeper.connect", "hadoopcm2:2181,hadoopcm3:2181,hadoopcm4:2181")
                        .property("bootstrap.servers", "hadoopcm1:9092,hadoopcm2:9092")
                ).withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())).createTemporaryTable("inputTable");

        Table sensorTable = tableEnv.from("inputTable");
        // 简单转换
        Table resultTable = sensorTable.select("id, temp")
                .filter("id === 'sensor_6'");


        // 连接kafka读取数据
        tableEnv.connect(new Kafka()
                        .version("0.11")
                        .topic("sinktest")
                        .property("zookeeper.connect", "hadoopcm2:2181,hadoopcm3:2181,hadoopcm4:2181")
                        .property("bootstrap.servers", "hadoopcm1:9092,hadoopcm2:9092")
                ).withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE())).createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");

        env.execute();
    }
}
