package io.kubesure.aggregate.job;

import org.apache.flink.api.java.utils.ParameterTool;

import io.kubesure.aggregate.util.Util;

public class TestUtil {
    public static void main(String args[]) throws Exception{
        ParameterTool ptool = Util.readProperties();
        System.out.println(ptool.getRequired("kafka.input.topic"));
        System.out.println(ptool.getProperties().size());
    }
    
}