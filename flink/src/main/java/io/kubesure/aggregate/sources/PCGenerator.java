package io.kubesure.aggregate.sources;

import org.joda.time.DateTime;

import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.datatypes.ProspectCompany.Builder;

public class PCGenerator {
    public static ProspectCompany generateProspectCompany(long id){
        Builder builder = ProspectCompany.newBuilder();
        builder.setId(id);
        builder.setCompanyName("Skynight Inc");
        builder.setTradeLicenseNumber("Trd4534rF");
        builder.setMatch(false);
        builder.setEventTime(new DateTime().getMillis());
        return builder.build();
    }

    private ProspectCompany newProspectCompany(long transactionID){
        ProspectCompany pc = PCGenerator.generateProspectCompany(transactionID);
        pc.setShareHolders(PGenerator.generateProspects());
        return pc;
    }
}
