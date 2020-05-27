package io.kubesure.aggregate.job;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.datatypes.Prospect;
import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.util.Convertor;
import io.kubesure.aggregate.util.Kafka;

public class TestEventTime {

    private static final Logger log = LoggerFactory.getLogger(TestEventTime.class);
    
    public static void main(String args[]) throws Exception {
        //testSequentialEventsWithCount(Time.seconds(2),10);  
        testOOOEvents(Time.seconds(2),35);
    }

    private static void testSequentialEvents(Time withDelay) throws Exception {
        while (true) {
            send(payLoad1());
            Thread.sleep(withDelay.toMilliseconds());
        }
    }

    private static void testOOOEvents(Time withDelay, int count) throws Exception {
        List<String> lateEvents = new ArrayList<String>(); 
        for (int i = 0; i < count; i++) {
            if (i / 2 == 0) {
                lateEvents.add(payLoad1());
            } else {
                send(payLoad1());
            }
            Thread.sleep(withDelay.toMilliseconds());
        }
        Thread.sleep(Time.seconds(5).toMilliseconds());
        for (String lateEvent : lateEvents) {
            log.info("-----------------lateEvent----------------------");
            log.info(lateEvent);
            send(lateEvent);
            Thread.sleep(Time.seconds(2).toMilliseconds());    
        }
    }

    private static void testSequentialEventsWithCount(Time withDelay, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            send(payLoad1());
            Thread.sleep(withDelay.toMilliseconds());
        }
    }

    private static String payLoad1() throws Exception {
        DateTime now = new DateTime(DateTimeZone.getDefault());
        ProspectCompany pc = new ProspectCompany(12345678l, "Skynight Inc", "Trd4534rF", false,now);
        Prospect p = new Prospect(789012l, "Prashant", "Patel", false);
        pc.addShareHolder(p); 
        String jsonPC = Convertor.convertProspectCompanyToJson(pc);
        log.info(jsonPC);
        return jsonPC;
    }

    private static String payLoad2() throws Exception {
        DateTime now = new DateTime(DateTimeZone.getDefault());
        ProspectCompany pc = new ProspectCompany(9876543l, "Karsandas & sons", "Trd4534rF", false,now);
        Prospect p = new Prospect(8743l, "Usha", "Patel", false);
        pc.addShareHolder(p); 
        String jsonPC = Convertor.convertProspectCompanyToJson(pc);
        log.info(jsonPC);
        return jsonPC;
    }

    private static void send(String payload) throws Exception {
        KafkaProducer<String,String> producer = Kafka.newKakfaProducer();
        ProducerRecord<String,String> producerRec =
             new ProducerRecord<String,String>("AggregateProspect", payload);
        try {
        	producer.send(producerRec).get();
        }catch(Exception kse){
        	log.error("Error writing message to dead letter Q", kse);
        }
    }
}