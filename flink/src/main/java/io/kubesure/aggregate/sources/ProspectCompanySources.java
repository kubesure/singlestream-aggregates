package io.kubesure.aggregate.sources;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.joda.time.DateTime;

import io.kubesure.aggregate.datatypes.Prospect;
import io.kubesure.aggregate.datatypes.ProspectCompany;

public class ProspectCompanySources extends RichSourceFunction<ProspectCompany>{

    private static final long serialVersionUID = -686876771747650202L;
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<ProspectCompany> ctx) throws Exception {
        ProspectCompany pc = new ProspectCompany(789l, "Skynight Inc", "Trd4534rF", false, new DateTime());
        Prospect p = new Prospect(123l, "Prashant", "Patel", false);
        pc.addShareHolder(p);

        while (running) {
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    
}
