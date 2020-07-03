package io.kubesure.aggregate.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.datatypes.Prospect;
import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.util.Convertor;

public class ProspectCompanySource implements SourceFunction<String>{

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

    private ProspectCompany newProspectCompany(long transactionID){
        ProspectCompany pc = new ProspectCompany(transactionID, "Skynight Inc", "Trd4534rF", false, new DateTime());
        Prospect p1 = new Prospect(123l, "Prashant", "Patel", false);
        Prospect p2 = new Prospect(124l, "Mukesh", "Patel", false);
        pc.addShareHolder(p1);
        pc.addShareHolder(p2);
        return pc;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (produce != 0) {
            for (int i = 0; i < 10; i++) {
                ProspectCompany pc = newProspectCompany(prospectCompanyStartID);
                ctx.collect(Convertor.convertProspectCompanyToJson(pc));
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
