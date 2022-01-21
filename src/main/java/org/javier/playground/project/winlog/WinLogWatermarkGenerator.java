package org.javier.playground.project.winlog;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import java.time.Duration;

public class WinLogWatermarkGenerator implements WatermarkGenerator<WinLog> {

    /** The maximum timestamp encountered so far. */
    private long maxTimestamp;

    /** The maximum out-of-orderness that this watermark generator assumes. */
    private final long outOfOrdernessMillis;

    private boolean flag;

    public WinLogWatermarkGenerator(Duration maxOutOfOrderness) {
        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();
        // start so that our lowest watermark would be Long.MIN_VALUE.
        this.flag = false;
        this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    @Override
    public void onEvent(WinLog event, long eventTimestamp, WatermarkOutput output) {
        System.out.println("[WatermarkGenerator] onEvent and update watermark");
        maxTimestamp = Math.max(maxTimestamp, event.occurTime);
        this.flag = true;
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
//        System.out.printf("[WatermarkGenerator] ID: %d, maxTimestamp: %d\n", Thread.currentThread().getId(), maxTimestamp);
        if(this.flag){
            this.flag = false;
            long wm = maxTimestamp - outOfOrdernessMillis;
            System.out.printf("[WatermarkGenerator] ID: %d, emitWaterMark: %d\n", Thread.currentThread().getId(), wm);
            output.emitWatermark(new Watermark(wm));
        }
    }
}