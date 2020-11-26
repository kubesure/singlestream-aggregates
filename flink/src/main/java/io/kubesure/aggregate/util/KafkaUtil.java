package io.kubesure.aggregate.util;

import java.util.Properties;

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.kubesure.aggregate.datatypes.ProspectCompany;

public class KafkaUtil {

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

	public static <T extends SpecificRecord> FlinkKafkaProducer<T> newFlinkAvroProducer
								(String topic, Class<T> tclass,String outputSchema,ParameterTool parameterTool){
		FlinkKafkaProducer<T> kafkaProducer = new FlinkKafkaProducer<>(
												parameterTool.getRequired(topic),  
												ConfluentRegistryAvroSerializationSchema.forSpecific(
												tclass,
												outputSchema,
												parameterTool.getRequired("schema.registry.url")),
												parameterTool.getProperties()
											);
		return kafkaProducer;									
	}

	public static FlinkKafkaConsumer<ProspectCompany> newFlinkAvroConsumer(ParameterTool parameterTool) {
		return	new FlinkKafkaConsumer<>(
					parameterTool.getRequired("kafka.input.topic"),  
					ConfluentRegistryAvroDeserializationSchema.forSpecific(
					ProspectCompany.class,
					parameterTool.getRequired("schema.registry.url")), 
					parameterTool.getProperties());
	}
    
}