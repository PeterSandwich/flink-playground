package org.javier.playground.project.winlog;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class MyEventTimeTrigger extends Trigger<Object, TimeWindow> {

    @Override
    public TriggerResult onElement(Object o, long l, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.printf("[Window Trigger] window结束边界=%d, currentWatermark==%d\n", window.maxTimestamp(), ctx.getCurrentWatermark());
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
        System.out.println("[Window Trigger] onProcessingTime");
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("[Window Trigger] onEventTime");
        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }


    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("[Window Trigger] clear");
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}
