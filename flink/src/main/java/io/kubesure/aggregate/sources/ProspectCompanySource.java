package io.kubesure.aggregate.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.util.PCGenerator;

public class ProspectCompanySource implements SourceFunction<ProspectCompany>{

    private static final long serialVersionUID = -686876771747650202L;
    private static final Logger log = LoggerFactory.getLogger(ProspectCompanySource.class);
    private long withDelay = 500l;
    private int produce;
    private long prospectCompanyStartID = 100;

    public ProspectCompanySource(int produce, int prospectCompanyStartID, long withDelay){
        this.produce = produce;
        this.withDelay = withDelay;
        this.prospectCompanyStartID = prospectCompanyStartID;
    }

    public ProspectCompanySource(int produce, long withDelay){
        this.produce = produce;
        this.withDelay = withDelay;
    }

    public ProspectCompanySource(){}

    @Override
    public void run(SourceContext<ProspectCompany> ctx) throws Exception {
        while (produce != 0) {
            for (int i = 0; i < 10; i++) {
                ctx.collect(PCGenerator.generateProspectCompany(i));
                Thread.sleep(withDelay);    
            }
            --produce;
        }
    }

    @Override
    public void cancel() {
        produce = 0;
    }
}
