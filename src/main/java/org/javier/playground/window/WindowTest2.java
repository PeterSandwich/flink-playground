package org.javier.playground.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class WindowTest2 {

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

        WatermarkStrategy<String> strategy = WatermarkStrategy.
                <String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> Long.parseLong(event));

        final OutputTag<SenorData> lateOutputTag = new OutputTag<SenorData>("late-data"){};

        SingleOutputStreamOperator<String> step1 = env.socketTextStream("localhost", 9999)
                .assignTimestampsAndWatermarks(strategy);
        SingleOutputStreamOperator<SenorData> step2 = step1.map(new MapFunction<String, SenorData>() {
            @Override
            public SenorData map(String s) throws Exception {
                return new SenorData("a", 1, Long.parseLong(s));
            }
        });

        SingleOutputStreamOperator<SenorData> result = step2
                .keyBy(SenorData::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .allowedLateness(Time.seconds(0))
                .sideOutputLateData(lateOutputTag)
                .process(new ProcessWindowFunction<SenorData, SenorData, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<SenorData> iterable, Collector<SenorData> collector) throws Exception {
                        System.out.println(context.window().getStart());
                        System.out.println(context.window().getEnd());
                        StringBuilder ret = new StringBuilder("[" + s + "]");
                        int size = 0;
                        for (SenorData senorData : iterable) {
                            ret.append("-").append(senorData.occurTime);
                            size++;
                        }
                        collector.collect(new SenorData(ret.toString(), size, 0L));
                    }
                });
        DataStream<SenorData> sideOutput = result.getSideOutput(lateOutputTag);
        result.print();
        sideOutput.print();
        env.execute("WindowTest2");
    }
}

