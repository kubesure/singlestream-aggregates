package io.kubesure.aggregate.job;

import javax.print.DocFlavor.INPUT_STREAM;

public class TestProspectMatches {

    public static void main(String args[]) {
        int id = 100;
        for (int i = 0; i < args.length; i++) {
            ProsepctCompanySource source = new ProsepctCompanySource(10,id,2000l);
            Thread t = new Thread(source);
            t.start();    
            id++;
        }
    }
}