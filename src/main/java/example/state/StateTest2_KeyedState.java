package example.state;

import example.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTest2_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(s -> {
            String[] split = s.split(", ");
            return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
        });

        // 定义一个有状态的map操作, 统计当前sensor数据个数
        SingleOutputStreamOperator<String> resultStream = dataStream.keyBy("id")
                        .map(new MyKeyCountMapper());

        resultStream.print();
        env.execute();
    }

    // 自定义richMapFunction
    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, String>{
        private ValueState<Integer> keyCountState;
        // 其他类型状态的声明
        private ListState<String> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class, 0));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("list-state", String.class));
        }

        @Override
        public String map(SensorReading sensorReading) throws Exception {
            // 其他状态api调用
            for (String str : listState.get()) {
                System.out.println(str);
            }

            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);
            return sensorReading.getId()+":"+count;
        }
    }

}
