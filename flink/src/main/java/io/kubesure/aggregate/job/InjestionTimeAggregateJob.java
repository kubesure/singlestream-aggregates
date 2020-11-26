package io.kubesure.aggregate.job;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.datatypes.AggregatedProspectCompany;
import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.sources.ProspectCompanySource;
import io.kubesure.aggregate.util.KafkaUtil;
import io.kubesure.aggregate.util.Util;


/**
 * @author Prashant Patel
 * InjestionTimeAggregateJob aggregates matches on Injestion time. Match provider publishes prospect
 * match event to topic kafka.input.topic, with isMatch attribute indicating match(a.k.a hit). 
 * Matches are aggregated/reduced on prospect Id as a key allowing concurrent aggregation of 
 * prospects. This job aggregates on fixed window time of window.time.seconds. 
 * late event and out of order events are not handled. Invalid matchs due to bad formed 
 * event message are delivered to kafka.DQL.topic for re-processing. Match events are aggregated 
 * and results published to kafka.sink.results.topic for consumer applications.         
 */

public class InjestionTimeAggregateJob {

	private static final Logger log = LoggerFactory.getLogger(InjestionTimeAggregateJob.class);
	private static ParameterTool parameterTool;

	public static void main(String[] args) throws Exception {

		parameterTool = Util.readProperties();
		StreamExecutionEnvironment env = Util.prepareExecutionEnv(parameterTool);
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// uncomment for unit testing. Tests only for one prospect company matches
		//DataStream<ProspectCompany> input = env.addSource(new ProspectCompanySource(1,101,2000l));

		// Comment for unit testing
		// Pulls message from kafka.input.topic maps to ProspectCompany
		DataStream<ProspectCompany> input = env
						.addSource(KafkaUtil.newFlinkAvroConsumer(parameterTool))
						.uid("Input");				
						
		// Consumed events are parsed to ProspectCompany for aggregation 				
		DataStream<AggregatedProspectCompany> prospectStream =	input
						.flatMap(new AggreateProspectCompany())
						.uid("FlatMapToProspectCompany");

		// ProspectCompany's are keyed by id and reduced to resuts over a time period of
		//window.time.seconds. Late events are keyed and aggregated without late processing.  
		DataStream<AggregatedProspectCompany> keyedPCStreams = prospectStream
						.keyBy(r -> r.getId())
						.timeWindow(Time.seconds
							(Integer.parseInt(parameterTool.getRequired("window.time.seconds"))))
						.reduce(new ProspectCompanyReduce())
						.uid("AggregateProspects");

		keyedPCStreams.print();				

		//Results are push to kafka skin kafka.sink.results.topic 				
		keyedPCStreams.addSink(KafkaUtil.newFlinkAvroProducer(
											parameterTool.getRequired("kafka.sink.results.topic"),
											AggregatedProspectCompany.class,
											parameterTool.getRequired("output.result.subject"),
											parameterTool))
		 			  .uid("ToKafkaSink");	

		env.execute("injestion time aggregation");
	}

	//Reduces match to matches
	private static class ProspectCompanyReduce implements ReduceFunction<AggregatedProspectCompany> {

		private static final long serialVersionUID = -686876771747650202L;

		public AggregatedProspectCompany reduce(
			                              AggregatedProspectCompany agc1, AggregatedProspectCompany agc2) {
			agc1.getCompanies().addAll(agc2.getCompanies());								  
			return agc1;
		}
	}

	private static class AggreateProspectCompany implements FlatMapFunction<ProspectCompany, AggregatedProspectCompany> {

		private static final long serialVersionUID = -686876771747690202L;

		@Override
		public void flatMap(ProspectCompany prospectCompany,  Collector<AggregatedProspectCompany> collector){

			KafkaProducer<String,String> producer = null;
			ProducerRecord<String,String> producerRec = null;	
			try {
				AggregatedProspectCompany apc = new AggregatedProspectCompany();
				List<ProspectCompany> prospectCompanies = new ArrayList<ProspectCompany>();
				prospectCompanies.add(prospectCompany);
				apc.setCompanies(prospectCompanies);
				apc.setId(prospectCompany.getId());
				collector.collect(apc);
			} catch (Exception e) {
				log.error("Error deserialzing Prospect company", e);
				producer = KafkaUtil.newKakfaProducer();
				// TODO: Define new avro error message payload 
				producerRec = new ProducerRecord<String,String>
								   (parameterTool.getRequired("kafka.DQL.topic"),
								    e.getMessage());
				// TODO: Implement as sideoutput to kafka using avro format
				try {
					producer.send(producerRec).get();
				}catch(Exception kse){
					log.error("Error writing message to dead letter Q", kse);
				}
			}finally{
				if(producer != null){
					producer.close();
				}				
			}
		}
	}
}
