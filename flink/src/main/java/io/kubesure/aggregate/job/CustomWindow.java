package io.kubesure.aggregate.job;

import java.util.Collection;
import java.util.Collections;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.datatypes.TimeType;

public class CustomWindow extends WindowAssigner<ProspectCompany, TimeWindow> {

   
    private static final long serialVersionUID = 192759827500753L;

    private long windowSize = 30_000L;
    private TimeType timeType;

    public CustomWindow(TimeType time){
        this.timeType = time;
    }

    @Override
    public Collection<TimeWindow> assignWindows(ProspectCompany element, long timestamp,
            WindowAssignerContext context) {
        long startTime = timestamp - (timestamp % windowSize);
        long endTime = startTime + windowSize;
        // emitting the corresponding time window
        return Collections.singletonList(new TimeWindow(startTime, endTime));
    }

    @Override
    public Trigger<ProspectCompany, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return new EarlyLateFireTrigger(timeType);
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        if (TimeType.EVENT == timeType) {
            return true;
        }
        return false;
    }
}