package org.javier.playground.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 数据源来自socket
 * 前置条件：
 * 开启监听端口： nc -l 9999
 */
public class SocketSource {

    static String HOST = "localhost";
    static int PORT = 9999;

    public static void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> step1 = env.socketTextStream(HOST, PORT);
        step1.print();
        env.execute("SocketSource");
    }
}
