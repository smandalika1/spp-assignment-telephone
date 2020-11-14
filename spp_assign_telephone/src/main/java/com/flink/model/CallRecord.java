package com.flink.model;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.Random;

@SuppressWarnings("serial")
public class CallRecord implements KafkaDeserializationSchema<CallKafkaRecord>
{
	@Override
	public boolean isEndOfStream(CallKafkaRecord nextElement) {
		return false;
	}
	
	@Override
	public CallKafkaRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
		CallKafkaRecord data = new CallKafkaRecord();
		data.areaCode =  new Random().ints(5, 0, 11).toArray()[0]; 
		data.fromPhoneNumber=generatePhonenumber();
		data.toPhoneNumber=generatePhonenumber();
		data.timestamp = record.timestamp();
		return data;
	}

	private String generatePhonenumber() {
		Random rand = new Random();

	    int num1, num2, num3;

	    num1 = rand.nextInt (900) + 100;
	    num2 = rand.nextInt (643) + 100;
	    num3 = rand.nextInt (9000) + 1000;

	    return (num1+"-"+num2+"-"+num3);
	}
	@Override
	public TypeInformation<CallKafkaRecord> getProducedType() {
		return TypeInformation.of(CallKafkaRecord.class);
	}
}
