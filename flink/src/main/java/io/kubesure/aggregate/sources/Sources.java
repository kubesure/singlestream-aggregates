package io.kubesure.aggregate.sources;

import java.util.Calendar;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import io.kubesure.aggregate.datatypes.Prospect;
import io.kubesure.aggregate.datatypes.ProspectCompany;

public class Sources {

    public static DataStream<ProspectCompany> ProspectCompanySource(StreamExecutionEnvironment env){

        DataStream<ProspectCompany> prospects = env.addSource(new SourceFunction<ProspectCompany>() {
            private volatile boolean running = true;

            @Override
            public void run(SourceContext<ProspectCompany> sc) throws Exception {

                ProspectCompany pc = new ProspectCompany("789", "Skynight Inc", "Trd4534rF", false);
                Prospect p = new Prospect("123", "Prashant", "Patel", false, Calendar.getInstance().getTimeInMillis());
                pc.addShareHolder(p);
                sc.collectWithTimestamp(pc, p.getTimestamp());
                Thread.sleep(1000);

                while (running) {
					Thread.sleep(1000);
				}
            }

            @Override
            public void cancel() {
                running = false;
            }
        });
        return prospects;
    }
    
}