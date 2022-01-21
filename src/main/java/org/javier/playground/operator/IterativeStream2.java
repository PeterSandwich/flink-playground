package org.javier.playground.operator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class IterativeStream2 {
    public static void test()throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        List<Long> collects = new ArrayList<>();
        collects.add(1L);
        collects.add(-4L);
        collects.add(2L);
        collects.add(-3L);
        collects.add(7L);

        DataStreamSource<Long> initialStream = env.fromCollection(collects);

        // 创建迭代流
        IterativeStream<Long> iteration =initialStream.iterate();

        // 增加处理逻辑，对元素执行减一操作。
        DataStream<Long> minusOne =iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value - 1 ;
            }
        });

        // 获取要进行迭代的流，
        DataStream<Long> stillGreaterThanZero= minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value > 0);
            }
        });

        // 对需要迭代的流形成一个闭环
        iteration.closeWith(stillGreaterThanZero);

        // 小于等于0的数据继续向前传输
        DataStream<Long> lessThanZero =minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value <= 0);
            }
        });
        initialStream.print();
        stillGreaterThanZero.print();
        env.execute("IterativeStream2");
    }
}
