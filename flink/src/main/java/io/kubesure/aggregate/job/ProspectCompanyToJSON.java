package io.kubesure.aggregate.job;

import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.util.Convertor;

//Converts late prospects to JSON for LateProspect sink 
class ProspectCompanyToJSON implements MapFunction<ProspectCompany,String> {

    private static final Logger log = LoggerFactory.getLogger(ProspectCompanyToJSON.class);

    private static final long serialVersionUID = -686876771747614202L;

    @Override
    public String map(ProspectCompany pc) throws Exception {
        try {
            return Convertor.convertProspectCompanyToJson(pc);
        } catch (Exception e) {
            // TODO: handle exception post error to dead letter for re-processing.
            log.error("Error serializing prospect company", e);
        }
        // TODO: Handle null as it breaks pipeline
        return null;
    }	
}