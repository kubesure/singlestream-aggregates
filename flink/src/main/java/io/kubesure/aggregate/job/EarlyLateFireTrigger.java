package io.kubesure.aggregate.job;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.datatypes.TimeType;

public class EarlyLateFireTrigger extends Trigger<ProspectCompany,TimeWindow> {

    private static final long serialVersionUID = 134333556753323L;
    private TimeType timeType;
    private long EARLY_FIRE = 10_000l;
    private long ALLOWED_LATENESS = 5_000L;

    public EarlyLateFireTrigger(TimeType timeType, long allowedLateNess){
        this.timeType = timeType;
        this.ALLOWED_LATENESS = allowedLateNess;
    }

    public EarlyLateFireTrigger(TimeType timeType){
        this.timeType = timeType;
    }

    @Override
    public TriggerResult onElement(ProspectCompany element, long timestamp, TimeWindow window, TriggerContext ctx)
            throws Exception {
        ValueState<Boolean> firstSeen = ctx.getPartitionedState(
                    new ValueStateDescriptor<>("firstSeen", Types.BOOLEAN));

        if (firstSeen.value() == null) {
            if(TimeType.PROCESSING == timeType) {
                registerProcessingTimeTimers(window, ctx);
            } else {
                registerProcessingEventTimers(window, ctx);
            }
            firstSeen.update(true);
        }                    
        return TriggerResult.CONTINUE;
    }

    private void registerProcessingTimeTimers(TimeWindow window, TriggerContext ctx) {
        // compute time for next early firing by rounding processing time to second
        long t = ctx.getCurrentProcessingTime() +
                        (EARLY_FIRE - (ctx.getCurrentProcessingTime() % EARLY_FIRE));
        ctx.registerProcessingTimeTimer(t);
        // register timer for the end of the window
        ctx.registerProcessingTimeTimer(window.getEnd() + ALLOWED_LATENESS);
    }

    private void registerProcessingEventTimers(TimeWindow window, TriggerContext ctx) {
        // compute time for next early firing by rounding watermark to second
        long t = ctx.getCurrentWatermark() + (EARLY_FIRE - (ctx.getCurrentWatermark() % EARLY_FIRE));
        ctx.registerEventTimeTimer(t);
        // register timer for the end of the window
        ctx.registerEventTimeTimer(window.getEnd() + ALLOWED_LATENESS);
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if(TimeType.EVENT == timeType) return TriggerResult.CONTINUE;

        if (time == window.getEnd()) {
            // final evaluation and purge window state
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            // register next early firing timer
            long earlyFireTime = ctx.getCurrentProcessingTime() + 
                                (EARLY_FIRE - (ctx.getCurrentProcessingTime() % EARLY_FIRE));
            if (earlyFireTime < window.getEnd()) {
                ctx.registerProcessingTimeTimer(earlyFireTime);
            }
            // fire trigger to early evaluate window
            return TriggerResult.FIRE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if(TimeType.PROCESSING == timeType) return TriggerResult.CONTINUE;

        if (time == window.getEnd()) {
            // final evaluation and purge window state
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            // register next early firing timer
            long earlyFireTime = ctx.getCurrentWatermark() + 
                                (EARLY_FIRE - (ctx.getCurrentWatermark() % EARLY_FIRE));
            if (earlyFireTime < window.getEnd()) {
                ctx.registerEventTimeTimer(earlyFireTime);
            }
            // fire trigger to early evaluate window
            return TriggerResult.FIRE;
        }
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
         // clear trigger state
         ValueState<Boolean> firstSeen = ctx.getPartitionedState(
            new ValueStateDescriptor<>("firstSeen", Types.BOOLEAN));
        firstSeen.clear();
    }
}