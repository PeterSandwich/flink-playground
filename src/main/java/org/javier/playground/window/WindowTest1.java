package org.javier.playground.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class WindowTest1 {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class SenorData {
        public String id;
        public Integer count;
        public Long occurTime;
    }


    public static void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<SenorData> strategy = WatermarkStrategy.
                <SenorData>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> event.occurTime);

        final OutputTag<SenorData> lateOutputTag = new OutputTag<SenorData>("late-data"){};

//        List<SenorData> collects = new ArrayList<>();
//        collects.add(new SenorData("1", 1, 190L));
//        collects.add(new SenorData("1", 1, 999L));
//        collects.add(new SenorData("1", 1, 1000L));
//        collects.add(new SenorData("1", 1, 1999L));
//        collects.add(new SenorData("1", 1, 2000L));
//        collects.add(new SenorData("1", 1, 3000L));
//        collects.add(new SenorData("1", 1, 400000L));
//        collects.add(new SenorData("1", 1, 500000L));
//        collects.add(new SenorData("1", 1, 600000L));
//        collects.add(new SenorData("1", 1, 700000L));
//        collects.add(new SenorData("1", 1, 6L));
//        collects.add(new SenorData("1", 1, 7L));
//        collects.add(new SenorData("1", 1, 10L));

        List<String> collects = new ArrayList<>();
        collects.add("1000");
        collects.add("2000");
        collects.add("3000");
        collects.add("4000");
        collects.add("4999");
        collects.add("5999");
        collects.add("100");
        collects.add("200");
        collects.add("90000");


        SingleOutputStreamOperator<String> step1 = env.fromCollection(collects)


//        SingleOutputStreamOperator<String> step1 = env.socketTextStream("localhost", 9999)
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                            @Override
                            public long extractTimestamp(String time) {
                                return Long.parseLong(time);
                            }
                        });
        SingleOutputStreamOperator<SenorData> step2 = step1.map(new MapFunction<String, SenorData>() {
            @Override
            public SenorData map(String s) throws Exception {
                return new SenorData("a", 1, Long.parseLong(s));
            }
        });

        SingleOutputStreamOperator<SenorData> result = step2
//        SingleOutputStreamOperator<SenorData> result = step1.assignTimestampsAndWatermarks(strategy)
                .keyBy(SenorData::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .allowedLateness(Time.seconds(0))
                .sideOutputLateData(lateOutputTag)

                .process(new ProcessWindowFunction<SenorData, SenorData, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<SenorData> iterable, Collector<SenorData> collector) throws Exception {
                        System.out.println(context.window().getStart());
                        System.out.println(context.window().getEnd());
                        String ret = "[" + s + "]";
                        int size = 0;
                        for (SenorData senorData : iterable) {
                            ret += "-" + senorData.occurTime;
                            size++;
                        }
                        collector.collect(new SenorData(ret, size, 0L));
                    }
                });
        DataStream<SenorData> sideOutput = result.getSideOutput(lateOutputTag);
        result.print();
        sideOutput.print();
        env.execute("WindowTest1");
    }

}
