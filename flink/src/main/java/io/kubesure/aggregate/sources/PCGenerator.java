package io.kubesure.aggregate.sources;

import org.joda.time.DateTime;

import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.datatypes.ProspectCompany.Builder;

public class PCGenerator {
    public static ProspectCompany generateProspectCompany(long id){
        Builder builder = ProspectCompany.newBuilder()
        .setId(id)
        .setCompanyName("Skynight Inc")
        .setTradeLicenseNumber("Trd4534rF")
        .setMatch(false)
        .setEventTime(new DateTime().getMillis())
        .setShareHolders(PGenerator.generateProspects());
        return builder.build();
    }
}
