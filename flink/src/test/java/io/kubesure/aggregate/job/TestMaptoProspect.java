package io.kubesure.aggregate.job;

import static org.junit.Assert.assertFalse;

import com.google.gson.Gson;

import org.junit.Test;

import io.kubesure.aggregate.datatypes.Prospect;


class TestJsonProsectToMap {
    
    @Test    
    public void TestProspectMap() throws Exception {
        Prospect p = new Gson().fromJson("{\"id\": 12121, \"cif\": \"cif\",\"firstName\": \"Prashant\",\"lastName\": \"Patel\",\"match\": false }", Prospect.class); 
        assertFalse("not a match",p.isMatch());
    }
}