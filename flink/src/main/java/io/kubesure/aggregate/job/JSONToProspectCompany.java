package io.kubesure.aggregate.job;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.util.Convertor;
import io.kubesure.aggregate.util.KafkaUtil;

class JSONToProspectCompany implements FlatMapFunction<String, ProspectCompany> {

        private static final long serialVersionUID = -686876771747690202L;
        private static final Logger log = LoggerFactory.getLogger(JSONToProspectCompany.class);
        private ParameterTool parameterTool;	
        
        public JSONToProspectCompany(ParameterTool parameterTool){
            this.parameterTool = parameterTool;
        }

		@Override
		public void flatMap(String prospectCompany, Collector<ProspectCompany> collector) {

			KafkaProducer<String, String> producer = null;
			ProducerRecord<String, String> producerRec = null;

			try {
				ProspectCompany pc = Convertor.convertToProspectCompany(prospectCompany);
				collector.collect(pc);
			} catch (Exception e) {
				log.error("Error deserialzing Prospect company", e);
				producer = KafkaUtil.newKakfaProducer(parameterTool);
				// TODO: Define new error message payload instead of dumping exception message on DLQ
				producerRec = new ProducerRecord<String, String>
										(parameterTool.getRequired("kafka.DQL.topic"), 
										e.getMessage());
				// TODO: Implement async send
				try {
					producer.send(producerRec).get();
				} catch (Exception kse) {
					log.error("Error writing message to dead letter Q", kse);
				}
			} finally {
				if (producer != null) {
					producer.close();
				}
			}
		}
	}