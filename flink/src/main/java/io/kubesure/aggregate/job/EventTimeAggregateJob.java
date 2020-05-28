package io.kubesure.aggregate.job;

import java.util.Comparator;
import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.datatypes.AggregatedProspectCompany;
import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.util.Convertor;
import io.kubesure.aggregate.util.TimeUtil;

public class EventTimeAggregateJob {

	private static final Logger log = LoggerFactory.getLogger(EventTimeAggregateJob.class);
	
	private static OutputTag<ProspectCompany> lateEvents = new OutputTag<ProspectCompany>("late-prospects"){
		private static final long serialVersionUID = -686876771742345642L;
	};

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(500l);

		// TODO: Accept kafka configuration form paramtool
		// TODO: Parse json with custom schema or implement Arvo schema 
		DataStream<ProspectCompany> inputStream = env
		            	.addSource(
							new FlinkKafkaConsumer<>(
								"AggregateProspect", 
								new SimpleStringSchema(), 
								kafkaConsumerProperties()))
						.flatMap(new JSONToProspectCompany())
						.assignTimestampsAndWatermarks
							(new BoundedOutOfOrdernessTimestampExtractor<ProspectCompany>(Time.seconds(10)) {
								private static final long serialVersionUID = -686876346234753642L;	
								@Override
								public long extractTimestamp(ProspectCompany element) {
									log.info("New Event Time     - {}", TimeUtil.ISOString(element.getEventTime().getMillis()));
									return element.getEventTime().getMillis();
								}
						}).name("Input");
						
		SingleOutputStreamOperator<AggregatedProspectCompany> results = inputStream
	         				.keyBy(r -> r.getId())
							.timeWindow(Time.seconds(30))
							.sideOutputLateData(lateEvents)
							.allowedLateness(Time.seconds(5))
							.process(new AggregateResults())
							.name("Aggregate");
		
		results.getSideOutput(lateEvents).print();					
		results.getSideOutput(lateEvents)					 					 
							.map(new LateProspectCounter())
							.map(new ProspectCompanyToJSON())
							.addSink(newFlinkKafkaProducer("LateProspectCheck"))
							.name("Late prospect sink");

		results.map(new ResultsToJSON())
							.addSink(newFlinkKafkaProducer("ProspectAggregated"))
							.name("Results");

		env.execute("event time aggregation");
	}

	private static class LateProspectCounter extends RichMapFunction<ProspectCompany,ProspectCompany> {

		private static final long serialVersionUID = -686876771234123442L;
		private transient Counter counter;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.counter = getRuntimeContext()
						   .getMetricGroup()
						   .counter("LateProspectCounter");
		}
		
		@Override
		public ProspectCompany map(ProspectCompany prospectCompany) throws Exception {
			this.counter.inc();
			return prospectCompany;
		}
	}

	private static class AggregateResults extends ProcessWindowFunction<ProspectCompany,AggregatedProspectCompany,
																				Long,TimeWindow>{
		private static final long serialVersionUID = -686876771747690123L;

		@Override
		public void process(Long key,
				ProcessWindowFunction<ProspectCompany, 
				AggregatedProspectCompany, Long, TimeWindow>.Context context,
				Iterable<ProspectCompany> elements,
				Collector<AggregatedProspectCompany> out) throws Exception {
						
				//log.info("Window WMark time  - {}" , TimeUtil.ISOString(context.currentWatermark()));
				log.info("Window start time  - {}" , TimeUtil.ISOString(context.window().getStart()));

				AggregatedProspectCompany agpc = new AggregatedProspectCompany();
				for (ProspectCompany pc : elements) {
		        	//log.info("Agg Event time     - {}" , TimeUtil.ISOString(pc.getEventTime().getMillis()));
					agpc.addCompany(pc);
					agpc.setId(pc.getId());
				}
				agpc.gProspectCompanies().sort(Comparator.comparing(ProspectCompany::getEventTime));
				for (ProspectCompany pc1 : agpc.gProspectCompanies()) {
					log.info("Agg Event time     - {}" , TimeUtil.ISOString(pc1.getEventTime().getMillis()));
				}

				log.info("Window end time    - {}" , TimeUtil.ISOString(context.window().getEnd())); 
				out.collect(agpc);		
		}
	}

	private static class JSONToProspectCompany
									implements FlatMapFunction<String, ProspectCompany> {

		private static final long serialVersionUID = -686876771747690202L;		

		@Override
		public void flatMap(String prospectCompany, Collector<ProspectCompany> collector) {

			KafkaProducer<String, String> producer = null;
			ProducerRecord<String, String> producerRec = null;

			try {
				ProspectCompany pc = Convertor.convertToProspectCompany(prospectCompany);
				collector.collect(pc);
			} catch (Exception e) {
				log.error("Error deserialzing Prospect company", e);
				producer = newKakfaProducer();
				// TODO: Define new error message payload instead of dumping exception message on DQL
				producerRec = new ProducerRecord<String, String>("ProspectAggregated-dl", e.getMessage());
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

	private static class ResultsToJSON implements MapFunction<AggregatedProspectCompany,String> {

		private static final long serialVersionUID = -686876771747614202L;

		@Override
		public String map(AggregatedProspectCompany agpc) throws Exception {
			try {
				return Convertor.convertAggregatedProspectCompanyToJson(agpc);
			} catch (Exception e) {
				// TODO: handle exception post error to dead letter for re-processing.
				log.error("Error serializing aggregate prospect company", e);
			}
			// TODO: Handle null as it breaks pipeline 	
			return null;
		}	
	}

	private static class ProspectCompanyToJSON implements MapFunction<ProspectCompany,String> {

		private static final long serialVersionUID = -686876771747614202L;

		@Override
		public String map(ProspectCompany pc) throws Exception {
			try {
				return Convertor.convertProspectCompanyToJson(pc);
			} catch (Exception e) {
				// TODO: handle exception post error to dead letter for re-processing.
				log.error("Error serializing prospect company", e);
			}
			// TODO: Handle null as it breaks pipeline
			return null;
		}	
	}

	// TODO: Use Paramtool properties
	@Deprecated   
	private static KafkaProducer<String, String> newKakfaProducer() {
		Properties properties = kafkaProperties();
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}
	
	private static FlinkKafkaProducer<String> newFlinkKafkaProducer(String topic) {
		Properties propsProducer = kafkaProperties();
		// TODO: replace depricated constructor
		FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
			    topic,
				new SimpleStringSchema(),
				propsProducer);
		kafkaProducer.setWriteTimestampToKafka(true);
		return kafkaProducer;
	}

	private static Properties kafkaConsumerProperties() {
		Properties propsConsumer = kafkaProperties();
		propsConsumer.setProperty("group.id", "aggregateprospectgrp");
		return propsConsumer;
	}

	// TODO: Use Paramtool properties
	@Deprecated
	private static Properties kafkaProperties() {
		Properties propsConsumer = new Properties();
		propsConsumer.setProperty("bootstrap.servers", "localhost:9092");
		propsConsumer.setProperty("zookeeper.connect", "localhost:2181");
		return propsConsumer;
	}
}
