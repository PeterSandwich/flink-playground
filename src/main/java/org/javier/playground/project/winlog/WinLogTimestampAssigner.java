package org.javier.playground.project.winlog;

import org.apache.flink.api.common.eventtime.TimestampAssigner;

public class WinLogTimestampAssigner implements TimestampAssigner<WinLog>  {
    @Override
    public long extractTimestamp(WinLog element, long recordTimestamp) {
        // recordTimestamp 是源数据的本身的时间戳
        // 最终时间戳可以提取element中的时间
        return element.occurTime;
    }
}
