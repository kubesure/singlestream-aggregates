package io.kubesure.aggregate.job;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubesure.aggregate.datatypes.AggregatedProspectCompany;
import io.kubesure.aggregate.datatypes.ProspectCompany;
import io.kubesure.aggregate.datatypes.TimeType;
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
public class CustomWindowAggregateJob {

	private static final Logger log = LoggerFactory.getLogger(EventTimeAggregateJob.class);
	
	private static OutputTag<ProspectCompany> lateEvents = new OutputTag<ProspectCompany>("late-prospects"){
		private static final long serialVersionUID = -686876771742345642L;
	};

	public static ParameterTool parameterTool;

	public static void main(String[] args) throws Exception {
		parameterTool = Util.readProperties();
		StreamExecutionEnvironment env = Util.prepareExecutionEnv(parameterTool);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Pulls message from kafka.input.topic maps to ProspectCompany and watermarks 
		// are generated with a delay of 10 second over event time stamp.    
		DataStream<ProspectCompany> inputStream = env
		            	.addSource(KafkaUtil.newFlinkAvroConsumer(parameterTool))
						.assignTimestampsAndWatermarks
							(new BoundedOutOfOrdernessTimestampExtractor<ProspectCompany>(Time.seconds(10)) {
								private static final long serialVersionUID = -686876346234753642L;	
								@Override
								public long extractTimestamp(ProspectCompany element) {
									log.info("New Event Time     - {}", TimeUtil.ISOString(element.getEventTime()));
									return element.getEventTime();
								}
						}).uid("Input");

		// Watermarked events are keyed by prospect id and aggregated within window.time.seconds and lateness of window.time.lateness
		// total lateness is window.time.lateness + 10 seconds 				
		SingleOutputStreamOperator<AggregatedProspectCompany> results = inputStream
	         				.keyBy(r -> r.getId())
							.window(new CustomWindow(TimeType.PROCESSING))
							.trigger(new EarlyLateFireTrigger(TimeType.PROCESSING))
							.process(new AggregateResults())
							.uid("AggregateProspects");

		if(log.isInfoEnabled()) {
			results.getSideOutput(lateEvents).print();
		}

		// Late events are delivered to kafka.sink.lateevents.topic  
		results.getSideOutput(lateEvents)					 					 
							.map(new LateProspectCounter())
							.addSink(KafkaUtil.newFlinkAvroProducer
									(parameterTool.getRequired("kafka.sink.lateevents.topic"),
									ProspectCompany.class,
									parameterTool.getRequired("output.late.result.subject"),									  
							 		parameterTool))
							.uid("LateProspectSink");

		if(log.isInfoEnabled()) {
			results.print();
		}
		
		// Aggregated results with window.time.seconds are delivered to kafka sink
		results.addSink(KafkaUtil.newFlinkAvroProducer(
										parameterTool.getRequired("kafka.sink.results.topic"),
										AggregatedProspectCompany.class,
										parameterTool.getRequired("output.result.subject"),
										parameterTool))
				.uid("Results");

		env.execute("event time aggregation");
	}
}
