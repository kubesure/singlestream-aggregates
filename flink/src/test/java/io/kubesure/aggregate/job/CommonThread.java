package io.kubesure.aggregate.job;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.util.Util;

public class CommonThread {

    private static final Logger log = LoggerFactory.getLogger(CommonThread.class);

    protected void send(String payload,String topic) throws Exception {
        KafkaProducer<String,String> producer = Util.newKakfaProducer();
        ProducerRecord<String,String> producerRec =
             new ProducerRecord<String,String>(topic, payload);
        try {
        	producer.send(producerRec).get();
        }catch(Exception kse){
        	log.error("Error writing message to dead letter Q", kse);
        }
    }
}