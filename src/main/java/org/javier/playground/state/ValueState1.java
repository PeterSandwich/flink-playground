package org.javier.playground.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 一个简单的state操作
 */
public class ValueState1 {

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
        step1.keyBy(v->v.f0).flatMap(new CountValueAvg()).print();
        env.execute("ValueState1");
    }

    public static class CountValueAvg extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Double>> {

        private transient ValueState<Tuple2<Integer, Integer>> sum;
        @Override
        public void flatMap(Tuple2<String, Integer> in, Collector<Tuple2<String, Double>> out) throws Exception {
            Tuple2<Integer, Integer> current = sum.value();
            if(current == null){
                current = Tuple2.of(0, 0);
            }
            current.f0 += 1;
            current.f1 += in.f1;

            if(current.f0 >= 2){
                out.collect(Tuple2.of(in.f0, (double)current.f1/current.f0));
                sum.clear();
            }else{
                sum.update(current);
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<Integer, Integer>> descriptor = new ValueStateDescriptor<>(
                    "avg",
                    Types.TUPLE(Types.STRING, Types.INT)
//                    TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {})
                    );
            sum = getRuntimeContext().getState(descriptor);
        }
    }
}
