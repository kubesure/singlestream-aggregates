package io.kubesure.aggregate.job;

import com.google.gson.Gson;

import io.kubesure.aggregate.datatypes.Prospect;
import io.kubesure.aggregate.datatypes.ProspectCompany;

public class Test {
    public static void main(String args[]) {
        Prospect p = new Gson().fromJson("{\"id\": 12121, \"cif\": \"cif\",\"firstName\": \"Prashant\",\"lastName\": \"Patel\",\"match\": false }", Prospect.class); 
        ProspectCompany c = new Gson().fromJson("{\n  \"id\": 12345,\n  \"companyName\": \"skyknight\",\n  \"tradeLicenseNumber\": \"dd3SrrT\",\n  \"match\": false,\n  \"shareHolders\": [\n    {\n      \"id\": 12121,\n      \"cif\": \"cif\",\n      \"firstName\": \"Prashant\",\n      \"lastName\": \"Patel\",\n      \"match\": false\n    },\n    {\n      \"id\": 12121,\n      \"cif\": \"cif\",\n      \"firstName\": \"Prashant\",\n      \"lastName\": \"Patel\",\n      \"match\": false\n    }\n  ]\n}", ProspectCompany.class);
        System.out.println(p);
    }
    
}