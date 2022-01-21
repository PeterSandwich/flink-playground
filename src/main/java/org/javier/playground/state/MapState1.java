package org.javier.playground.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class MapState1 {

    public static void test()throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String, Integer>> collects = new ArrayList<>();
        collects.add(Tuple2.of("tom",4));
        collects.add(Tuple2.of("tom",1));
        collects.add(Tuple2.of("sunny",9));
        collects.add(Tuple2.of("tom",3));
        collects.add(Tuple2.of("sunny",2));
        collects.add(Tuple2.of("sunny",41));
        collects.add(Tuple2.of("tom",14));
        collects.add(Tuple2.of("sunny",12));

        DataStreamSource<Tuple2<String, Integer>> step1 = env.fromCollection(collects);
        step1.keyBy(v->v.f0).flatMap(new MapState1.CountValueAvg()).print();
        env.execute("MapState1");
    }

    public static class CountValueAvg extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Double>> {

        private transient MapState<String, Integer> sum;
        @Override
        public void flatMap(Tuple2<String, Integer> in, Collector<Tuple2<String, Double>> out) throws Exception {
            sum.put(UUID.randomUUID().toString(),in.f1);
            ArrayList<Integer> integers = Lists.newArrayList(sum.values());

            if(integers.size() >= 2){
                Integer c = 0;
                Integer s = 0;
                for (Integer integer : integers) {
                    c+=1;
                    s+=integer;
                }
                sum.clear();
                out.collect(Tuple2.of(in.f0, (double)s/c));
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<String, Integer>(
                    "avg",
                    String.class,
                    Integer.class
            );
            sum = getRuntimeContext().getMapState(descriptor);
        }
    }
}
