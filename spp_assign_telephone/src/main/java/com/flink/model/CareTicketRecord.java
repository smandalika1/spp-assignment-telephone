package com.flink.model;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.Random;

@SuppressWarnings("serial")
public class CareTicketRecord implements KafkaDeserializationSchema<TicketKafkaRecord>
{
	@Override
	public boolean isEndOfStream(TicketKafkaRecord nextElement) {
		return false;
	}
	
	@Override
	public TicketKafkaRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
		TicketKafkaRecord data = new TicketKafkaRecord();
		data.areaCode =  new Random().ints(5, 0, 11).toArray()[0]; 
		data.fromPhoneNumber=generatePhonenumber();
		data.timestamp = record.timestamp();
		data.complaintType="BAD_NETWORK";
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
	public TypeInformation<TicketKafkaRecord> getProducedType() {
		return TypeInformation.of(TicketKafkaRecord.class);
	}
}
