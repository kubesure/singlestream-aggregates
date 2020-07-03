package io.kubesure.aggregate.job;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.datatypes.Prospect;
import io.kubesure.aggregate.datatypes.ProspectCompany;
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
                ProspectCompany pc = newProspectCompany(id);
                send(Convertor.convertProspectCompanyToJson(pc), "AggregateProspect");
                Thread.sleep(INTERVAL_TIME);
            } catch (InterruptedException txp) {
                log.error("Error sleeping thread", txp);  
            }   
            catch (Exception e) {
                log.error("Error sending payment to kafka", e);
            }
        }
    }

    private ProspectCompany newProspectCompany(long transactionID){
        ProspectCompany pc = new ProspectCompany(transactionID, "Skynight Inc", "Trd4534rF", false, new DateTime());
        Prospect p1 = new Prospect(123l, "Prashant", "Patel", false);
        Prospect p2 = new Prospect(124l, "Mukesh", "Patel", false);
        pc.addShareHolder(p1);
        pc.addShareHolder(p2);
        return pc;
    }
}