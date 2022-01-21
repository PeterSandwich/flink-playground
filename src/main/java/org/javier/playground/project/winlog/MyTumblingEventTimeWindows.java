package org.javier.playground.project.winlog;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;

public class MyTumblingEventTimeWindows extends TumblingEventTimeWindows {
    protected MyTumblingEventTimeWindows(long size, long offset, WindowStagger windowStagger) {
        super(size, offset, windowStagger);
    }

    @Override
    public Collection<TimeWindow> assignWindows(
            Object element, long timestamp, WindowAssignerContext context) {
        return super.assignWindows(element, timestamp, context);
    }

    public static MyTumblingEventTimeWindows of(Time size) {
        return new MyTumblingEventTimeWindows(size.toMilliseconds(), 0, WindowStagger.ALIGNED);
    }
}
