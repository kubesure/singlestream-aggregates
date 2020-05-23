package io.kubesure.aggregate.sources;

import java.util.Calendar;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import io.kubesure.aggregate.datatypes.Prospect;
import io.kubesure.aggregate.datatypes.ProspectCompany;

public class ProspectCompanySources extends RichSourceFunction<ProspectCompany>{

    private static final long serialVersionUID = -686876771747650202L;
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<ProspectCompany> ctx) throws Exception {
        ProspectCompany pc = new ProspectCompany("789", "Skynight Inc", "Trd4534rF", false);
        Prospect p = new Prospect("123", "Prashant", "Patel", false, Calendar.getInstance().getTimeInMillis());
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
