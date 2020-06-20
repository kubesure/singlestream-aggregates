package io.kubesure.aggregate.job;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

import io.kubesure.aggregate.datatypes.ProspectCompany;

/**
	 * Late prospect dashboard counter is incremented by 1 on encountering a late event 
**/
class LateProspectCounter extends RichMapFunction<ProspectCompany, ProspectCompany> {

    private static final long serialVersionUID = -686876771234123442L;
    private transient Counter counter;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.counter = getRuntimeContext()
                       .getMetricGroup()
                       .counter("LateProspectCounter");
    }
    
    @Override
    public ProspectCompany map(ProspectCompany prospectCompany) throws Exception {
        this.counter.inc();
        return prospectCompany;
    }
}