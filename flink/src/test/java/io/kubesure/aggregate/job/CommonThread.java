package io.kubesure.aggregate.job;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.util.KafkaUtil;

public class CommonThread {

    private static final Logger log = LoggerFactory.getLogger(CommonThread.class);

    protected void sendProspectCompany(ProspectCompany payload,String topic) throws Exception {
        KafkaProducer<String,ProspectCompany> producer = KafkaUtil.newKakfaAvroProducer();
        ProducerRecord<String,ProspectCompany> producerRec =
             new ProducerRecord<String,ProspectCompany>(topic, payload);
        try {
        	producer.send(producerRec).get();
        }catch(Exception kse){
        	log.error("Error writing message to Prospect Company topic", kse);
        }
    }
}