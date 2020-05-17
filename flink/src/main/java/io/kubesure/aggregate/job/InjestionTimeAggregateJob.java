package io.kubesure.aggregate.job;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.datatypes.AggregatedProspectCompany;
import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.util.Convertor;

public class InjestionTimeAggregateJob {

	private static final Logger log = LoggerFactory.getLogger(InjestionTimeAggregateJob.class);

	public static void main(String[] args) throws Exception {

		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// DataStream<ProspectCompany> customerStream = Sources.customerSource(env);
		DataStream<ObjectNode> kafkaProspectStream;
		try {
			// TODO: accept kafka configuration form command line params.
			Properties propsConsumer = new Properties();
			propsConsumer.setProperty("bootstrap.servers", "localhost:9092");
			propsConsumer.setProperty("zookeeper.connect", "localhost:2181");
			propsConsumer.setProperty("group.id", "aggregateprospectgrp");
			kafkaProspectStream = env.addSource(
				new FlinkKafkaConsumer<>(
								"AggregateProspect", 
								new JSONKeyValueDeserializationSchema(false), 
								propsConsumer),
			"Kafka-prospect-stream-source");	
		} catch (Exception kce) {
			//TODO: handle exception
			log.error("Error reading kafka message from Topic AggregateProspect", kce);
			throw kce;
		}
		

		DataStream<AggregatedProspectCompany> prospectStream =	
		                                            kafkaProspectStream.map(new MapToAggreateProspectCompany());

		DataStream<AggregatedProspectCompany> keyedPCStreams = prospectStream
		.keyBy(r -> r.getId())
		.timeWindow(Time.minutes(3))
		.reduce(new AggregatedProspectCompanyReduce());

		keyedPCStreams.print();

		DataStream<String> aggregatedStream = keyedPCStreams.map(new MapAggregatedProspectToString());
		
		aggregatedStream.print();

		try {
			Properties propsProducer = new Properties();
			propsProducer.setProperty("bootstrap.servers", "localhost:9092");
			propsProducer.setProperty("zookeeper.connect", "localhost:2181");
			// TODO: replace depricated constuctor   
			FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
						"ProspectAggregated", new SimpleStringSchema(),propsProducer
			);
			kafkaProducer.setWriteTimestampToKafka(true);
			aggregatedStream.addSink(kafkaProducer);	
		} catch (Exception kpe) {
			log.error("Error sending message to sink topic", kpe);
		}
		
		env.execute("injestion-time-aggregation");
	}

	private static class AggregatedProspectCompanyReduce implements ReduceFunction<AggregatedProspectCompany> {
		public AggregatedProspectCompany reduce(
			                              AggregatedProspectCompany agc1, AggregatedProspectCompany agc2) {
			agc1.addCompany(agc2.getProspectCompany());
			return agc1;
		}
	}

	private static class MapToAggreateProspectCompany implements MapFunction<ObjectNode, AggregatedProspectCompany> {
		private static final long serialVersionUID = -6867736771747690202L;

		@Override
		public AggregatedProspectCompany map(ObjectNode node) throws Exception {

			try {
				log.info("prospect> " + node.get("value").toString());
				ProspectCompany pc = Convertor.convertToProspectCompany(node.get("value").toString());
				AggregatedProspectCompany apc = new AggregatedProspectCompany();
				apc.addCompany(pc);
				apc.setId(pc.getId());
				return apc;	
			} catch (Exception e) {
				// TODO: handle exception post error to dead letter for re-processing.
				log.error("Error deserialzing Prospect Company", e);
			}
			return null;
		}
	}

	private static class MapAggregatedProspectToString implements MapFunction<AggregatedProspectCompany, String> {
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

	// TODO: not used remove. 
	private static class MapToProspect implements MapFunction<ObjectNode, ProspectCompany> {

		@Override
		public ProspectCompany map(ObjectNode node) throws Exception {
			try {
				log.info("prospect>" + node.get("value").toString());
				ProspectCompany pc = Convertor.convertToProspectCompany(node.get("value").toString());
				return pc;
			} catch (Exception e) {
				// TODO: handle exception post error to dead letter for re-processing.
				log.error("Error deserialzing Prospect Company", e);
			}
			return null;
		}
	}

	// TODO: KeyProcessFunction not required. remove.
	private static class ProspectCompanyFunction
			extends KeyedProcessFunction<String, ProspectCompany, ProspectCompany> {
		@Override
		public void open(Configuration config) {

		}

		@Override
		public void processElement(ProspectCompany pc, Context context, Collector<ProspectCompany> out)
				throws Exception {
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext context, Collector<ProspectCompany> out) throws Exception {
		}
	}
}
