package io.kubesure.aggregate.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.sources.PCGenerator;
import io.kubesure.aggregate.util.Convertor;


public class ProsepctCompanySource extends CommonThread implements Runnable {

    private long id = 1;
    private int produce; 
    private long INTERVAL_TIME = 3000l;
    private static final Logger log = LoggerFactory.getLogger(ProsepctCompanySource.class);

    public ProsepctCompanySource(int produce,long withDelay){
        this.produce = produce;
        this.INTERVAL_TIME = withDelay;
    }

    public ProsepctCompanySource(int produce,int prospectCompanyStartID,long withDelay){
        this.produce = produce;
        this.INTERVAL_TIME = withDelay;
        this.id = prospectCompanyStartID;
    }

    @Override
    public void run() {

        for (int i = 0; i < produce; i++) {
            try {
                ProspectCompany pc = PCGenerator.generateProspectCompany(id);
                sendProspectCompany(pc, "AggregateProspect");
                Thread.sleep(INTERVAL_TIME);
            } catch (InterruptedException txp) {
                log.error("Error sleeping thread", txp);  
            }   
            catch (Exception e) {
                e.printStackTrace();
                System.out.println(e.getMessage());
                log.error("Error sending payment to kafka", e);
            }
        }
    }
}