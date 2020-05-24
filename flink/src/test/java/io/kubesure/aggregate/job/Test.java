package io.kubesure.aggregate.job;

import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.util.Convertor;

public class Test {
    public static void main(String args[]) {
        try {
            ProspectCompany c = Convertor.convertToProspectCompany("{\"id\": 12345,\"companyName\": \"skyknight\",\"tradeLicenseNumber\": \"dd3SrrT\",\"match\": false,\"eventTime\" : \"2020-05-22 02:16:44\",\"shareHolders\": [{\"id\": 12121,\"cif\": \"cif\",\"firstName\": \"Prashant\",\"lastName\": \"Patel\",\"match\": false},{\"id\": 12121,\"cif\":\"cif\",\"firstName\": \"Prashant\",\"lastName\": \"Patel\",\"match\": false}]}");
            System.out.println(c);    
        } catch (Exception e) {

            System.out.println(e);
        }
    }
}