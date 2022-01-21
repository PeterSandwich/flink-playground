package org.javier.playground.operator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 测试Iterate这种操作方式
 */
public class IterativeStream1 {
    
    public static void test()throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 不定义并行度还会出错？
        env.setParallelism(1);
        List<Long> collects = new ArrayList<>();
        collects.add(1L);
        collects.add(-4L);
        collects.add(2L);
        collects.add(-3L);
        collects.add(7L);

        DataStreamSource<Long> initialStream = env.fromCollection(collects);
        IterativeStream<Long> iteration = initialStream.iterate();
        DataStream<Long> feedback = iteration.filter(new FilterFunction<Long>(){
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 0;
            }
        });
        iteration.closeWith(feedback);

        // 这样只会输出小于等于0的方式
        DataStream<Long> output = iteration.filter(new FilterFunction<Long>(){
            @Override
            public boolean filter(Long value) throws Exception {
                return value <= 0;
            }
        });
        output.print();
        feedback.print();
        env.execute("IterativeStream1");
    }
}
