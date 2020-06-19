package io.kubesure.aggregate.job;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
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
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.datatypes.AggregatedProspectCompany;
import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.util.Convertor;
import io.kubesure.aggregate.util.KafkaUtil;
import io.kubesure.aggregate.util.TimeUtil;
import io.kubesure.aggregate.util.Util;


/**
 * @author Prashant Patel
 * EventTimeAggregateJob aggregates prospect matches on event time . Match provider publishes prospect
 * match event to topic kafka.input.topic, with isMatch attribute indicating match(a.k.a hit). 
 * Matches are aggregated on prospect Id as a key allowing concurrent aggregation of prospects.
 * This job is event time pipleline and produces late prospects which were delayed by 
 * window.time.seconds + window.time.lateness. Late prospects are pushed to topic 
 * kafka.sink.lateevents.topic and lateness counter is incremented and dashboard updated. 
 * Invalid matchs due to bad formed event message are delivered to kafka.DQL.topic for 
 * re-processing. Matche events are aggregated and results published to kafka.sink.results.topic 
 * for consumer applications.         
 */
public class EventTimeAggregateJob {

	private static final Logger log = LoggerFactory.getLogger(EventTimeAggregateJob.class);
	
	private static OutputTag<ProspectCompany> lateEvents = new OutputTag<ProspectCompany>("late-prospects"){
		private static final long serialVersionUID = -686876771742345642L;
	};

	public static ParameterTool parameterTool;

	public static void main(String[] args) throws Exception {
		parameterTool = Util.readProperties();
		StreamExecutionEnvironment env = Util.prepareExecutionEnv(parameterTool);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// TODO: Parse json with custom schema or implement Arvo schema
		// Pulls message from kafka.input.topic maps to ProspectCompany and watermarks 
		// are generated with a delay of 10 second over event time stamp.    
		DataStream<ProspectCompany> inputStream = env
		            	.addSource(
							new FlinkKafkaConsumer<>(
								parameterTool.getRequired("kafka.input.topic"), 
								new SimpleStringSchema(), 
								parameterTool.getProperties()))
						.flatMap(new JSONToProspectCompany())
						.assignTimestampsAndWatermarks
							(new BoundedOutOfOrdernessTimestampExtractor<ProspectCompany>(Time.seconds(10)) {
								private static final long serialVersionUID = -686876346234753642L;	
								@Override
								public long extractTimestamp(ProspectCompany element) {
									log.info("New Event Time     - {}", TimeUtil.ISOString(element.getEventTime().getMillis()));
									return element.getEventTime().getMillis();
								}
						}).uid("Input");

		// Watermarked events are keyed by prospect id and aggregated within window.time.seconds and lateness of window.time.lateness
		// total lateness is window.time.lateness + 10 seconds 				
		SingleOutputStreamOperator<AggregatedProspectCompany> results = inputStream
	         				.keyBy(r -> r.getId())
							.timeWindow(Time.seconds
							            (Integer.parseInt(parameterTool.getRequired("window.time.seconds"))))
							.sideOutputLateData(lateEvents)
							.allowedLateness(Time.seconds
							            (Integer.parseInt(parameterTool.getRequired("window.time.lateness"))))
							.process(new AggregateResults())
							.uid("AggregateProspects");

		if(log.isInfoEnabled()) {
			results.getSideOutput(lateEvents).print();
		}

		// Late events are delivered to kafka.sink.lateevents.topic  
		results.getSideOutput(lateEvents)					 					 
							.map(new LateProspectCounter())
							.map(new ProspectCompanyToJSON())
							.addSink(KafkaUtil.newFlinkKafkaProducer
									(parameterTool.getRequired("kafka.sink.lateevents.topic"),
							 		parameterTool))
							.uid("LateProspetcSink");

		if(log.isInfoEnabled()) {
			results.print();
		}
		
		// Aggregated results with window.time.seconds are delivered to kafka sink
		results.map(new ResultsToJSON())
							.addSink(KafkaUtil.newFlinkKafkaProducer
									(parameterTool.getRequired("kafka.sink.results.topic"),
							  		 parameterTool))
							.uid("Results");

							

		env.execute("event time aggregation");
	}

	/**
	 * Late prospect dashboard counter is incremented by 1 on encountering a late event 
	 */
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

	/**
	 * On window end time prospect are aggregated and sorted for sink operator to process  
	 */
	private static class AggregateResults extends ProcessWindowFunction<ProspectCompany,AggregatedProspectCompany,
																				Long,TimeWindow>{
		private static final long serialVersionUID = -686876771747690123L;

		@Override
		public void process(Long key,
				ProcessWindowFunction<ProspectCompany, 
				AggregatedProspectCompany, Long, TimeWindow>.Context context,
				Iterable<ProspectCompany> elements,
				Collector<AggregatedProspectCompany> out) throws Exception {

				if(log.isInfoEnabled()){
					log.info("Window start time  - {} - {}" , 
											   TimeUtil.ISOString(context.window().getStart()),
											   context.window().hashCode());	
				}	

				AggregatedProspectCompany agpc = new AggregatedProspectCompany();
				for (ProspectCompany pc : elements) {
					agpc.addCompany(pc);
					agpc.setId(pc.getId());
				}

				//remove duplicate elements generated for late events arrived between
				//window.time.seconds and window.time.lateness 
				List<ProspectCompany> distinctProspectCo = agpc.getProspectCompanies()
				 									           .stream()
													           .distinct()
													           .collect(Collectors.toList());
				agpc.setProspectCompanies(distinctProspectCo);									   

				//Sort out of order event by event time 
				agpc.getProspectCompanies().sort(Comparator.comparing(ProspectCompany::getEventTime));

				if(log.isInfoEnabled()){
					for (ProspectCompany pc1 : agpc.getProspectCompanies()) {
						log.info("Agg Event time     - {}" , TimeUtil.ISOString(pc1.getEventTime().getMillis()));
					}
					log.info("Window end time    - {}" , TimeUtil.ISOString(context.window().getEnd())); 
				}

				out.collect(agpc);		
		}
	}

	//Converts each prosepct in the pipleline from JSON to ProspectCompany POJO
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

	// Converts aggregated matches to JSON for sink processing
	private static class ResultsToJSON implements MapFunction<AggregatedProspectCompany,String> {

		private static final long serialVersionUID = -686876771747614202L;

		@Override
		public String map(AggregatedProspectCompany agpc) throws Exception {
			try {
				return Convertor.convertAggregatedProspectCompanyToJson(agpc);
			} catch (Exception e) {
				// TODO: Handle exception post error to dead letter for re-processing
				log.error("Error serializing aggregate prospect company", e);
			}
			// TODO: Handle null as it breaks pipeline 	
			return null;
		}	
	}

	//Converts late prospects to JSON for LateProspect sink 
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
}
