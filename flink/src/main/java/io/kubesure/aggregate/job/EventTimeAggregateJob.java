package io.kubesure.aggregate.job;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
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

public class EventTimeAggregateJob {

	private static final Logger log = LoggerFactory.getLogger(EventTimeAggregateJob.class);
	
	private static OutputTag<ProspectCompany> lateEvents = new OutputTag<ProspectCompany>("late-prospects"){
		private static final long serialVersionUID = -686876771742345642L;
	};

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(500l);

		// TODO: accept kafka configuration form paramtool
		// TODO: Parse json with custom schema 
		Properties propsConsumer = kafkaConsumerProperties();
		DataStream<ProspectCompany> inputStream = env
		            .addSource(
						new FlinkKafkaConsumer<>(
							"AggregateProspect", 
							new SimpleStringSchema(), 
							propsConsumer))
					.flatMap(new JSONToProspectCompany())
					.assignTimestampsAndWatermarks
						(new BoundedOutOfOrdernessTimestampExtractor<ProspectCompany>(Time.seconds(10)) {
							private static final long serialVersionUID = -686876346234753642L;	
							@Override
							public long extractTimestamp(ProspectCompany element) {
								return element.getEventTime().getMillis();
							}
						}).name("Input");
						
		SingleOutputStreamOperator<ProspectCompany> lateProspectFiltered = inputStream
						.process(new LateProspectFilter());

		lateProspectFiltered.getSideOutput(lateEvents)
							.map(new ProspectCompanyToJSON())
							.addSink(newFlinkKafkaProducer("LateProspectCheck"))
							.name("Late prospect sink");

		DataStream<AggregatedProspectCompany> results = lateProspectFiltered
		        	.keyBy(r -> r.getId())
					.timeWindow(Time.seconds(30))
					.process(new AggregateResults()).name("Aggregate");
		
		results.map(new ResultsToJSON())
						.addSink(newFlinkKafkaProducer("ProspectAggregated")).name("Results");
						
		env.execute("event time aggregation");
	}

	private static class LateProspectFilter extends ProcessFunction<ProspectCompany,ProspectCompany>{
		private static final long serialVersionUID = -686876771234753642L;	

		@Override
		public void processElement(ProspectCompany prospectCompany,
								   ProcessFunction<ProspectCompany, 
								   ProspectCompany>.Context ctx, 
								   Collector<ProspectCompany> out) throws Exception {
			if(prospectCompany.getEventTime().getMillis() < ctx.timerService().currentWatermark()) {
				ctx.output(lateEvents, prospectCompany);
			}else {
				out.collect(prospectCompany);
			}	
		}
	}

	private static class AggregateResults extends ProcessWindowFunction<ProspectCompany,AggregatedProspectCompany,
																				Long,TimeWindow>{
		private static final long serialVersionUID = -686876771747690123L;

		@Override
		public void process(Long key,
				ProcessWindowFunction<ProspectCompany, AggregatedProspectCompany, Long, TimeWindow>.Context context,
				Iterable<ProspectCompany> elements, Collector<AggregatedProspectCompany> out) throws Exception {

				AggregatedProspectCompany agpc = new AggregatedProspectCompany();
				for (ProspectCompany pc : elements) {
					agpc.addCompany(pc);
					agpc.setId(pc.getId());
				} 
				out.collect(agpc);		
		}
	}

	public static class BoundedOutOfOrdernessGenerator implements 
	                                            AssignerWithPeriodicWatermarks<ProspectCompany> {
		private static final long serialVersionUID = -686873471234753642L;	

		private long maxOutOfOrderness = 3500; //default 3.5 seconds 
		private long currentMaxTimestamp;

		private BoundedOutOfOrdernessGenerator(){};

		private BoundedOutOfOrdernessGenerator(Time time){
			maxOutOfOrderness = time.toMilliseconds();
		}
	
		@Override
		public long extractTimestamp(ProspectCompany element, long previousElementTimestamp) {
			long timestamp = element.getEventTime().getMillis();
			currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
			return timestamp;
		}
	
		@Override
		public Watermark getCurrentWatermark() {
			return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
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
				// TODO: Define new error message payload
				producerRec = new ProducerRecord<String, String>("ProspectAggregated-dl", e.getMessage());
				// TODO: Implement a async send
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
			// TODO: Handle null pipeline breaks
			return null;
		}	
	}

	private static KafkaProducer<String, String> newKakfaProducer() {
		Properties properties = kafkaProperties();
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}

	private static FlinkKafkaProducer<String> newFlinkKafkaProducer(String topic) {
		Properties propsProducer = kafkaProperties();
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

	private static Properties kafkaProperties() {
		Properties propsConsumer = new Properties();
		propsConsumer.setProperty("bootstrap.servers", "localhost:9092");
		propsConsumer.setProperty("zookeeper.connect", "localhost:2181");
		return propsConsumer;
	}
}
