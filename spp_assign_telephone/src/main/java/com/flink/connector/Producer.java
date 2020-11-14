package com.flink.connector;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.flink.model.CallRecord;

public class Producer<T> 
{
	String bootstrapServers;
	KafkaProducer<String, T> producer;
	String topic;
	
	public Producer(String kafkaServer, String serializerName,String topic)
	{
		this.bootstrapServers = kafkaServer;
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerName);

        // create the producer
        producer = new KafkaProducer<String, T>(properties);
        this.topic=topic;
	}
	
	public void send( T message)
	{
        // create a producer record
        ProducerRecord<String, T> record = new ProducerRecord<String, T>(topic, "key", message);

        // send data - asynchronous
        producer.send(record);
        
        // flush data
        producer.flush();
	}
	
	public void close()
	{
		// flush and close producer
        producer.close();
	}
}