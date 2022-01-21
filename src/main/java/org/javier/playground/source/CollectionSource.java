package org.javier.playground.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class CollectionSource {

    public static void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<Integer, Integer>> collects = new ArrayList<>();
        collects.add(new Tuple2<>(1,2));
        collects.add(Tuple2.of(3,4));
        DataStreamSource<Tuple2<Integer, Integer>> step1 = env.fromCollection(collects);
        SingleOutputStreamOperator<Integer> step2 = step1.map(new MapFunction<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer map(Tuple2<Integer, Integer> in) throws Exception {
                return in.f0 + in.f1;
            }
        });
        step2.print();
        env.execute("CollectionSource");
    }

}
