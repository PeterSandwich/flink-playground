package org.javier.playground.state;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 状态后端，状态持久化，
 */
public class StateBackend1 {

    static String HOST = "localhost";
    static int PORT = 9999;

    public static void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> step1 = env.socketTextStream(HOST, PORT);

        // 开启checkpoint
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        // 保留checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 状态后端是本地文件系统，后面最好用配置文件配置
        env.setStateBackend(new FsStateBackend("file:///Users/panjiawei/code/flink-studio/word-count/checkpoints"));
        step1.print();
        env.execute("SocketSource");
    }
}
