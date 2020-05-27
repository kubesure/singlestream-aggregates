package io.kubesure.aggregate.job;

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
        while (true) {
            DateTime now = new DateTime(DateTimeZone.getDefault());
            ProspectCompany pc = new ProspectCompany(12345678l, "Skynight Inc", "Trd4534rF", false,now);
            Prospect p = new Prospect(789012l, "Prashant", "Patel", false);
            pc.addShareHolder(p); 
            String jsonPC = Convertor.convertProspectCompanyToJson(pc);
            log.info(jsonPC);
            KafkaProducer<String,String> producer = Kafka.newKakfaProducer();
            ProducerRecord<String,String> producerRec =
                 new ProducerRecord<String,String>("AggregateProspect", jsonPC);
			try {
				producer.send(producerRec).get();
			}catch(Exception kse){
				log.error("Error writing message to dead letter Q", kse);
            }
            Thread.sleep(Time.seconds(5).toMilliseconds());
        }
    }
}