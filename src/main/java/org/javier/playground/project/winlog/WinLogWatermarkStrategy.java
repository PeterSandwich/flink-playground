package org.javier.playground.project.winlog;

import org.apache.flink.api.common.eventtime.*;

import java.time.Duration;

public class WinLogWatermarkStrategy implements WatermarkStrategy<WinLog> {

    final private Duration  maxOutOfOrderness;
    public WinLogWatermarkStrategy(Duration maxOutOfOrderness){
        this.maxOutOfOrderness = maxOutOfOrderness;
    }

    @Override
    public WatermarkGenerator<WinLog> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WinLogWatermarkGenerator(this.maxOutOfOrderness);
    }

    @Override
    public TimestampAssigner<WinLog> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new WinLogTimestampAssigner();
    }

}
