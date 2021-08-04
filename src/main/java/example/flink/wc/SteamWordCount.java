package example.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SteamWordCount {
    public static void main(String[] args) throws Exception{
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(1);

        // 从文本中获取数据
        //DataStream<String> inputDataStream = env.readTextFile("/Users/lhl/Downloads/SparkDemo/src/main/resources/hello.txt");

        // 用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 从socket文本流获取数据
        DataStream<String> inputDataStream = env.socketTextStream(host, port);

        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }).keyBy(0).sum(1).setParallelism(2).slotSharingGroup("red");

        resultStream.print().setParallelism(1);



        env.execute();
    }
}
