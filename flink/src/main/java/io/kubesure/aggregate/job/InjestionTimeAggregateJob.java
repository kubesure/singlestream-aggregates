package io.kubesure.aggregate.job;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.datatypes.AggregatedProspectCompany;
import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.util.Convertor;
import io.kubesure.aggregate.util.Kafka;

public class InjestionTimeAggregateJob {

	private static final Logger log = LoggerFactory.getLogger(InjestionTimeAggregateJob.class);

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		
		// TODO: accept kafka configuration form command line params.
		Properties propsConsumer = kafkaConsumerProperties();
		DataStream<String> kafkaProspectStream = env.addSource(
													new FlinkKafkaConsumer<>(
														"AggregateProspect", 
														new SimpleStringSchema(), 
														propsConsumer),
														"Kafka-prospect-stream-source");	
		
		DataStream<AggregatedProspectCompany> prospectStream =	kafkaProspectStream
		.flatMap(new FlatMapToAggreateProspectCompany());

		DataStream<AggregatedProspectCompany> keyedPCStreams = prospectStream
		.keyBy(r -> r.getId())
		.timeWindow(Time.minutes(3))
		.reduce(new AggregatedProspectCompanyReduce());

		keyedPCStreams.print();

		DataStream<String> aggregatedStream = keyedPCStreams.map(new MapAggregatedProspectToString());
		
		aggregatedStream.print();

		try {
			FlinkKafkaProducer<String> kafkaProducer = newFlinkKafkaProducer();
			aggregatedStream.addSink(kafkaProducer);	
		} catch (Exception kpe) {
			log.error("Error sending message to sink topic", kpe);
		}
		env.execute("injestion time aggregation");
		
	}

	private static class AggregatedProspectCompanyReduce implements ReduceFunction<AggregatedProspectCompany> {

		private static final long serialVersionUID = -686876771747650202L;

		public AggregatedProspectCompany reduce(
			                              AggregatedProspectCompany agc1, AggregatedProspectCompany agc2) {
			agc1.addCompany(agc2.getProspectCompany());
			return agc1;
		}
	}

	private static class FlatMapToAggreateProspectCompany implements FlatMapFunction<String, AggregatedProspectCompany> {

		private static final long serialVersionUID = -686876771747690202L;

		@Override
		public void flatMap(String prospectCompany,  Collector<AggregatedProspectCompany> collector){

			KafkaProducer<String,String> producer = null;
			ProducerRecord<String,String> producerRec = null;	
			try {
				ProspectCompany pc = Convertor.convertToProspectCompany(prospectCompany);
				AggregatedProspectCompany apc = new AggregatedProspectCompany();
				apc.addCompany(pc);
				apc.setId(pc.getId());
				collector.collect(apc);
			} catch (Exception e) {
				log.error("Error deserialzing Prospect company", e);
				producer = Kafka.newKakfaProducer();
				// TODO: Define new error message payload 
				producerRec = new ProducerRecord<String,String>
								   ("ProspectAggregated-dl",e.getMessage());
				// TODO: Implement a async send
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

	private static class MapAggregatedProspectToString implements MapFunction<AggregatedProspectCompany, String> {

		private static final long serialVersionUID = -686876771747614202L;

		@Override
		public String map(AggregatedProspectCompany agpc) throws Exception {
			try {
				return Convertor.convertAggregatedProspectCompanyToJson(agpc);
			} catch (Exception e) {
				// TODO: handle exception post error to dead letter for re-processing.
				log.error("Error serialzing aggregate prospect company", e);
			}
			return null;
		}	
	}

	private static FlinkKafkaProducer<String> newFlinkKafkaProducer() {
		// TODO: replace depricated constuctor
		Properties propsProducer = new Properties();
		propsProducer.setProperty("bootstrap.servers", "localhost:9092");
		propsProducer.setProperty("zookeeper.connect", "localhost:2181");   
		FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
					"ProspectAggregated", new SimpleStringSchema(),propsProducer
		);
		kafkaProducer.setWriteTimestampToKafka(true);
		return kafkaProducer;
	}

	private static Properties kafkaConsumerProperties() {
		Properties propsConsumer = new Properties();
		propsConsumer.setProperty("bootstrap.servers", "localhost:9092");
		propsConsumer.setProperty("zookeeper.connect", "localhost:2181");
		propsConsumer.setProperty("group.id", "aggregateprospectgrp");
		return propsConsumer;
	}
}
