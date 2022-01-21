package org.javier.playground.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * kafka数据源，一个简单的例子，可以快速对接kafka
 * 复杂的位移功能等等后面再细化
 * 前置条件：
 * 1.brokers可以连通
 * 2.topic必须存在
 */
public class KafkaSource1 {

    public static void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> step1 = env.fromSource(KafkaSource1.getSource(), WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<String> step2 = step1.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return "pjw-" + s;
            }
        });

        step2.print();

        env.execute("KafkaSource1");
    }

    public static KafkaSource<String> getSource(){
        String brokers = "localhost:9092,localhost:9093,localhost:9094";
        return KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("input_2")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
