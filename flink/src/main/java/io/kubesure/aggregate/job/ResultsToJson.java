package io.kubesure.aggregate.job;

import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.datatypes.AggregatedProspectCompany;
import io.kubesure.aggregate.util.Convertor;

// Converts aggregated matches to JSON for sink processing
class ResultsToJSON implements MapFunction<AggregatedProspectCompany, String> {

    private static final Logger log = LoggerFactory.getLogger(ResultsToJSON.class);

    private static final long serialVersionUID = -686876771747614202L;

    @Override
    public String map(AggregatedProspectCompany agpc) throws Exception {
        try {
            return Convertor.convertAggregatedProspectCompanyToJson(agpc);
        } catch (Exception e) {
            // TODO: Handle exception post error to dead letter for re-processing
            log.error("Error serializing aggregate prospect company", e);
        }
        // TODO: Handle null as it breaks pipeline 	
        return null;
    }	
}