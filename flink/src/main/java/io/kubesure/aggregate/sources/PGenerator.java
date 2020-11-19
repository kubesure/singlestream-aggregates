package io.kubesure.aggregate.sources;


import java.util.ArrayList;
import java.util.List;

import io.kubesure.aggregate.datatypes.Prospect;
import io.kubesure.aggregate.datatypes.Prospect.Builder;

public class PGenerator {
    public static List<Prospect> generateProspects(){
        Builder prospectBuilder = Prospect.newBuilder();
        prospectBuilder.setId(123l);
        prospectBuilder.setFirstName("Prashant");
        prospectBuilder.setLastName("Patel");
        prospectBuilder.setMatch(false);
        Prospect p1 = prospectBuilder.build();

        prospectBuilder.setId(456l);
        prospectBuilder.setFirstName("Usha");
        prospectBuilder.setLastName("Patel");
        prospectBuilder.setMatch(false);
        Prospect p2 = prospectBuilder.build();
        List <Prospect> shareHolders = new ArrayList<Prospect>();
        shareHolders.add(p1);
        shareHolders.add(p2);
        return shareHolders;
    }
}