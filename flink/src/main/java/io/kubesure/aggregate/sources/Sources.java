package io.kubesure.aggregate.sources;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import io.kubesure.aggregate.datatypes.Prospect;
import io.kubesure.aggregate.datatypes.ProspectCompany;

public class Sources {

    public static DataStream<ProspectCompany> customerSource(StreamExecutionEnvironment env){

        DataStream<ProspectCompany> customers = env.addSource(new SourceFunction<ProspectCompany>() {
            private volatile boolean running = true;

            @Override
            public void run(SourceContext<ProspectCompany> sc) throws Exception {

                ProspectCompany pc = new ProspectCompany("789", "Skynight Inc", "Trd4534rF", false);
                pc.addShareHolder(new Prospect("123", "Prashant", "Patel", false));
                sc.collectWithTimestamp(pc,0);
                sc.emitWatermark(new Watermark(0));
                Thread.sleep(2000);

                while (running) {
					Thread.sleep(1000);
				}
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        return customers;
    }
}
