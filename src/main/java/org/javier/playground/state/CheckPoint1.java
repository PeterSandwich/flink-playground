package org.javier.playground.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * 检测点尝试
 * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/fault-tolerance/checkpointing/#enabling-and-configuring-checkpointing
 */
public class CheckPoint1 {

    static String HOST = "localhost";
    static int PORT = 9999;

    public static void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 当9999端口没有开启，没有checkpoint则马上会挂掉，有了checkpoint会一直重试
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        // 重启策略：
        // 不开启checkpoint不重启
        // 开启checkpoint：
        //      1. 没有定义策略，则重试int.max，固定间隔一秒
        //      2. 定义了策略按照策略来
        // 重启策略配置： 1. 代码方式 2. flink_conf.yaml配置文件方式
        // https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/ops/state/task_failure_recovery/
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(10, TimeUnit.SECONDS) // delay
        ));

        // 重启策略在wordcount的表现
        // 重启之后状态保存，还会接着上面的状态计数, 注意：但是重启如果是关了停掉又没有将状态持久化，重启后还是没有
        DataStreamSource<String> step1 = env.socketTextStream(HOST, PORT);
        step1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                if(s.equals("pk")){
                    throw new RuntimeException("有人要pk，完了！");
                }
                for (String s1 : s.split(",")) {
                    collector.collect(s1);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        })
                .keyBy(s -> s.f0)
                .sum(1)
                .print();
        env.execute("SocketSource");
    }
}
