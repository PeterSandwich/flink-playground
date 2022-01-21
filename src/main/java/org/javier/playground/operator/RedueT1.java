package org.javier.playground.operator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;

import java.util.ArrayList;
import java.util.List;

public class RedueT1 {
    public static void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<String, Integer>> collects = new ArrayList<>();
        collects.add(Tuple2.of("tom",4));
        collects.add(Tuple2.of("sun",1));
        collects.add(Tuple2.of("sun",2));

        DataStreamSource<Tuple2<String, Integer>> step1 = env.fromCollection(collects);
        step1.keyBy(0).reduce(new AggregationFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> in1, Tuple2<String, Integer> in2) throws Exception {
                if(in1.f0.equals(in2.f0)){
                    System.out.println("equal"+in2.f0);
                }
                return Tuple2.of(in1.f0, in1.f1+in2.f1);
            }
        }).print();
        env.execute("RedueT1");
    }
}
