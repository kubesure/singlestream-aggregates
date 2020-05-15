package io.kubesure.aggregate.util;

import com.google.gson.Gson;

import io.kubesure.aggregate.datatypes.AggregatedProspectCompany;
import io.kubesure.aggregate.datatypes.Prospect;
import io.kubesure.aggregate.datatypes.ProspectCompany;

public class Convertor {
    public static Prospect convertToProspect(String prospect) throws Exception {
        return new Gson().fromJson(prospect, Prospect.class);
    }
    
    public static ProspectCompany convertToProspectCompany(String prospectCompany) throws Exception{
        return new Gson().fromJson(prospectCompany, ProspectCompany.class);
    }

    public static String convertAggregatedProspectCompanyToJson(AggregatedProspectCompany agpc) throws Exception{
        return new Gson().toJson(agpc);
    }
}