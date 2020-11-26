package io.kubesure.aggregate.util;

import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.kubesure.aggregate.datatypes.ProspectCompany;

public class Util {

	public static KafkaProducer<String,String> newKakfaProducer(){
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties); 
		return producer;
	}

	public static KafkaProducer<String,ProspectCompany> newKakfaAvroProducer(){
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.put("key.serializer",StringSerializer.class.getName());
		properties.put("value.serializer",KafkaAvroSerializer.class.getName());
		properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

		KafkaProducer<String,ProspectCompany> producer = new KafkaProducer<String,ProspectCompany>(properties); 
		return producer;
	}

	public static ParameterTool readProperties() throws Exception {
		//ParameterTool parameterTool = ParameterTool.fromPropertiesFile("prospectstream.properties");
		
		
		ParameterTool parameterTool = ParameterTool.fromPropertiesFile
								(Util.class.getClassLoader().getResourceAsStream("stream.properties"));
		return parameterTool;
	}
	
	public static StreamExecutionEnvironment prepareExecutionEnv(ParameterTool parameterTool)
		throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setAutoWatermarkInterval(500l);
		// env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		// env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
		env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
		return env;
	}
}