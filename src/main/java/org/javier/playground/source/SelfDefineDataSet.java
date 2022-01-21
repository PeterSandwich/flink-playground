package org.javier.playground.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义来源，可以自己设置数据集合，做一些测试，对数据源解耦合
 */
public class SelfDefineDataSet {

    public static void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> step1 = env.addSource(new SelfDefineDataSource(), "SelfDefineDataSource");
        step1.print();
        env.execute("SelfDefineDataSet");
    }

    public static class SelfDefineDataSource implements SourceFunction<String> {

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            sourceContext.collect("test-1");
            sourceContext.collect("test-2");
            sourceContext.collect("test-3");
            sourceContext.collect("test-4");
            sourceContext.collect("test-5");
            sourceContext.collect("test-6");
        }

        @Override
        public void cancel() {
            System.out.println("cancel");
        }
    }
}
