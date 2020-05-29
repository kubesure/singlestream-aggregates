package io.kubesure.aggregate.job;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
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
import io.kubesure.aggregate.util.KafkaUtil;
import io.kubesure.aggregate.util.Util;

public class InjestionTimeAggregateJob {

	private static final Logger log = LoggerFactory.getLogger(InjestionTimeAggregateJob.class);
	private static ParameterTool parameterTool;

	public static void main(String[] args) throws Exception {

		parameterTool = Util.readProperties();
		StreamExecutionEnvironment env = Util.prepareExecutionEnv(parameterTool);
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		
		// TODO: accept kafka configuration form command line params
		DataStream<String> kafkaProspectStream = env
						.addSource(
							new FlinkKafkaConsumer<>(
							parameterTool.getRequired("kafka.input.topic"),  
							new SimpleStringSchema(), 
							parameterTool.getProperties()))
						.name("Input");	
		
		DataStream<AggregatedProspectCompany> prospectStream =	kafkaProspectStream
						.flatMap(new AggreateProspectCompany())
						.name("Flat map to prospect company");

		DataStream<AggregatedProspectCompany> keyedPCStreams = prospectStream
						.keyBy(r -> r.getId())
						.timeWindow(Time.seconds
							(Integer.parseInt(parameterTool.getRequired("window.time.seconds"))))
						.reduce(new ProspectCompanyReduce())
						.name("Aggregate");

		DataStream<String> aggregatedStream = keyedPCStreams
						.map(new AggregatedProspectToString())
						.name("Prospect to JSON");
		
		FlinkKafkaProducer<String> kafkaProducer = KafkaUtil.newFlinkKafkaProducer
												   (parameterTool.getRequired("kafka.sink.results.topic"),
												   parameterTool);
		aggregatedStream.addSink(kafkaProducer)
						.name("To kafka sink");	

		env.execute("injestion time aggregation");
	}

	private static class ProspectCompanyReduce implements ReduceFunction<AggregatedProspectCompany> {

		private static final long serialVersionUID = -686876771747650202L;

		public AggregatedProspectCompany reduce(
			                              AggregatedProspectCompany agc1, AggregatedProspectCompany agc2) {
			agc1.addCompany(agc2.getProspectCompany());
			return agc1;
		}
	}

	private static class AggreateProspectCompany implements FlatMapFunction<String, AggregatedProspectCompany> {

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
				producer = Util.newKakfaProducer();
				// TODO: Define new error message payload 
				producerRec = new ProducerRecord<String,String>
								   (parameterTool.getRequired("kafka.DQL.topic"),
								    e.getMessage());
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

	private static class AggregatedProspectToString implements MapFunction<AggregatedProspectCompany, String> {

		private static final long serialVersionUID = -686876771747614202L;

		@Override
		public String map(AggregatedProspectCompany agpc) throws Exception {
			try {
				return Convertor.convertAggregatedProspectCompanyToJson(agpc);
			} catch (Exception e) {
				// TODO: handle exception post error to dead letter for re-processing
				log.error("Error serialzing aggregate prospect company", e);
			}
			return null;
		}	
	}	
}
