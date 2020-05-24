package io.kubesure.aggregate.job;

import java.util.Properties;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
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

		// TODO: accept kafka configuration form paramtool.
		Properties propsConsumer = kafkaConsumerProperties();
		DataStream<ProspectCompany> input = env
		            .addSource(
						new FlinkKafkaConsumer<>(
							"AggregateProspect", 
							new SimpleStringSchema(), 
							propsConsumer))
					.flatMap(new FlatMapToAggreateProspectCompany())
					.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());

																		   
		DataStream<AggregatedProspectCompany> aggregateStream = input
		        				.keyBy(r -> r.getId())
								.timeWindow(Time.seconds(30))
								.sideOutputLateData(lateEvents)
								.process(new ProspectCompanyProcessFunction());
		aggregateStream.print("Print aggregated prospect company");
		
		//DataStream<String> aggregatedStream = keyedPCStreams.map(new MapAggregatedProspectToString());
		//aggregatedStream.print("Print JSON aggregated prospects");
 
		/*try {
			FlinkKafkaProducer<String> kafkaProducer = newFlinkKafkaProducer();
			aggregatedStream.addSink(kafkaProducer).uid("Merged kafka stream");
		} catch (Exception kpe) {
			log.error("Error sending message to sink topic", kpe);
		}*/
		env.execute("event time aggregation");

	}

		
	public static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<ProspectCompany> {
		private static final long serialVersionUID = -686873471234753642L;	
		private long maxOutOfOrderness = 5000; // 3.5 seconds
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

	private static class ProspectCompanyProcessFunction extends ProcessWindowFunction<ProspectCompany,AggregatedProspectCompany,
																				Long,TimeWindow>{

		private static final long serialVersionUID = -686876771747690123L;

		@Override
		public void process(Long key,
				ProcessWindowFunction<ProspectCompany, AggregatedProspectCompany, Long, TimeWindow>.Context context,
				Iterable<ProspectCompany> elements, Collector<AggregatedProspectCompany> out) throws Exception {

				AggregatedProspectCompany agpc = new AggregatedProspectCompany();
				for (ProspectCompany pc : elements) {
						agpc.addCompany(pc);
				} 
				out.collect(agpc);		
		}
	}

	private static class FlatMapToAggreateProspectCompany
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

	private static class MapAggregatedProspectToString implements MapFunction<AggregatedProspectCompany, String> {

		private static final long serialVersionUID = -686876771747614202L;

		@Override
		public String map(AggregatedProspectCompany agpc) throws Exception {
			try {
				// return Convertor.convertAggregatedProspectCompanyToJson(agpc);
				return "";
			} catch (Exception e) {
				// TODO: handle exception post error to dead letter for re-processing.
				log.error("Error serialzing aggregate prospect company", e);
			}
			return null;
		}	
	}

	private static class LateProspectFilter extends ProcessFunction<ProspectCompany,ProspectCompany>{
		private static final long serialVersionUID = -686876771234753642L;	
			@Override
			public void processElement(ProspectCompany prospectCompany,
					ProcessFunction<ProspectCompany, ProspectCompany>.Context ctx, Collector<ProspectCompany> out)
					throws Exception {
						if(prospectCompany.getEventTime().getMillis() < ctx.timerService().currentWatermark()) {
							ctx.output(lateEvents, prospectCompany);
						}	
			}
	}

	private static class KeyedWindowedAggregatedProspectProcess extends 
									ProcessWindowFunction<ProspectCompany, AggregatedProspectCompany,Long,TimeWindow	> {

		private static final long serialVersionUID = -686876771757650202L;				
		
		@Override
		public void process
					(Long key,
					 Context context,
					 Iterable<ProspectCompany> inputs,
					 Collector<AggregatedProspectCompany> out) {

			ProspectCompany pc = inputs.iterator().next();
			if(pc.getEventTime().getMillis() < context.currentProcessingTime()) {
				context.output(lateEvents, pc);
			} else {
				AggregatedProspectCompany agpc = new AggregatedProspectCompany();
				for (ProspectCompany itrPc : inputs) {
					agpc.addCompany(itrPc);
				} 
				out.collect(agpc);	
			}	
		}
	}

	private static class AggregatedProspectCompanyReduce implements ReduceFunction<AggregatedProspectCompany> {

		private static final long serialVersionUID = -686876771747650202L;

		public AggregatedProspectCompany reduce(AggregatedProspectCompany agc1, AggregatedProspectCompany agc2) {
			agc1.addCompany(agc2.getProspectCompany());
			return agc1;
		}
	}

	private static class ProspectAccumalator implements AggregateFunction
	                                           <ProspectCompany,AggregatedProspectCompany,AggregatedProspectCompany> {

		private static final long serialVersionUID = -686124671747650642L;										
		private static final Logger log = LoggerFactory.getLogger(ProspectAccumalator.class);

		@Override
		public AggregatedProspectCompany createAccumulator() {
			return new AggregatedProspectCompany();
		}

		@Override
		public AggregatedProspectCompany add(ProspectCompany value, AggregatedProspectCompany accumulator) {
			accumulator.addCompany(value);
			return accumulator;
		}

		@Override
		public AggregatedProspectCompany getResult(AggregatedProspectCompany accumulator) {
			return accumulator;
		}

		@Override
		public AggregatedProspectCompany merge(AggregatedProspectCompany a, AggregatedProspectCompany b) {
			return a;
		}

	}

	private static KafkaProducer<String, String> newKakfaProducer() {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}

	private static FlinkKafkaProducer<String> newFlinkKafkaProducer() {
		// TODO: replace depricated constuctor
		Properties propsProducer = new Properties();
		propsProducer.setProperty("bootstrap.servers", "localhost:9092");
		propsProducer.setProperty("zookeeper.connect", "localhost:2181");
		FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>("ProspectAggregated",
				new SimpleStringSchema(), propsProducer);
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
