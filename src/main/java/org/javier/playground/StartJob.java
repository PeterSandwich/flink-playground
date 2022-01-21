package org.javier.playground;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.javier.playground.project.winlog.WinLogDetect;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class StartJob {
    public static void main(String[] args) {
        try {
            WinLogDetect.Main(args);
//            WindowTest2.test();
        }catch (Exception e){
            System.out.println(e.toString());
        }
    }

    public static void simpleJop()throws Exception {
        // 1.创建一个简单的环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 定义输入源
        DataStreamSource<String> dataStreamSource = env.addSource(
                new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        Random random = new Random();
                        // 循环可以不停的读取静态数据
                        while (true) {
                            int nextInt = random.nextInt(100);
                            ctx.collect("random : " + nextInt);
                            Thread.sleep(1000);
                        }
                    }
                    @Override
                    public void cancel() {}
                },
                "simple-source");

        // 3.map操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = dataStreamSource.map(
                new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] sps = value.split(":");
                        return new Tuple2<>(value, Integer.parseInt(sps[1].trim()));
                    }
                }
        );

        // 4.filter操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> filter = map.filter(
                new FilterFunction<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> in) throws Exception {
                        return in.f1%2==1;
                    }
                }
        );

        // 5.输出
        filter.addSink(
                new SinkFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void invoke(Tuple2<String, Integer> value) throws Exception {
                        LoggerFactory.getLogger(this.getClass()).info(value.f1.toString());
                    }
                }
        );

        // 5.1 或打印
        // filter.print();

        // 6.最终执行
        try {
            env.execute("main");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
