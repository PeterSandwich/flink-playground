package org.javier.playground.project.winlog;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;
import java.util.Properties;

public class WinLogDetect {


    public static void Main(String[] args)throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000);
        DataStreamSource<WinLog> source = GetKafkaSource(env);
        source.print();
        SingleOutputStreamOperator<Tuple2<WinLog, Long>> map = source.map(in -> Tuple2.of(in, 1L))
                .returns(TypeInformation.of(new TypeHint<Tuple2<WinLog, Long>>() {}));

        map.keyBy(new KeySelector<Tuple2<WinLog, Long>, String>() {
            @Override
            public String getKey(Tuple2<WinLog, Long> value) throws Exception {
                return value.f0.getIp()+"#"+value.f0.getName();
            }
        })
                .window(MyTumblingEventTimeWindows.of(Time.hours(2))).trigger(new MyEventTimeTrigger())
//                .process(new ProcessWindowFunction<Tuple2<WinLog, Long>, Tuple2<String, Long>, String, TimeWindow>() {
//                    @Override
//                    public void process(String s, Context context, Iterable<Tuple2<WinLog, Long>> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
//                        System.out.println(context.window().getStart());
//                        System.out.println(context.window().getEnd());
//                        Long time = 0L;
//                        Long count = 0L;
//                        for (Tuple2<WinLog, Long> in : iterable) {
//                            if(in.f0.occurTime>time){
//                                time = in.f0.occurTime;
//                            }
//                            count += in.f1;
//                        }
//                        collector.collect(Tuple2.of(s, count));
//                    }
//                }).print();
                .reduce(new ReduceFunction<Tuple2<WinLog, Long>>() {
                    @Override
                    public Tuple2<WinLog, Long> reduce(Tuple2<WinLog, Long> value1, Tuple2<WinLog, Long> value2) throws Exception {
                        return Tuple2.of(
                                new WinLog(value1.f0.ip, value1.f0.name, value1.f0.occurTime>value2.f0.occurTime?value1.f0.occurTime:value2.f0.occurTime),
                                value1.f1+value2.f1);
                    }
                })
                .print();
        env.execute("WinLogDetect");
    }

    public static DataStreamSource<WinLog> getSource(StreamExecutionEnvironment env){
        String brokers = "localhost:9092,localhost:9093,localhost:9094";
        KafkaSource<WinLog> build = KafkaSource.<WinLog>builder()
                .setBootstrapServers(brokers)
                .setTopics("input_3")
                .setGroupId("gg1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SourceWinLogSchema())
                .build();
        return env.fromSource(
                build,
                new WinLogWatermarkStrategy(Duration.ofSeconds(0)),
                "winlog-Kafka-Source");

    }

    public static DataStreamSource<WinLog> GetKafkaSource(StreamExecutionEnvironment env){
        String topic = "input2";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        prop.setProperty("group.id","gg1");

        FlinkKafkaConsumer<WinLog> myConsumer = new FlinkKafkaConsumer<>(topic, new SourceWinLogSchema(), prop);
        myConsumer.setStartFromLatest();//默认消费策略
        myConsumer.assignTimestampsAndWatermarks(new WinLogWatermarkStrategy(Duration.ofMinutes(10)));
        return env.addSource(myConsumer);
    }

}
