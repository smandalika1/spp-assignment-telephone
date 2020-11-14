package com.flink;

import java.time.LocalTime;
import java.util.Properties;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.flink.connector.Producer;
import com.flink.model.CareTicketRecord;
import com.flink.model.TicketGenerator;
import com.flink.model.TicketKafkaRecord;
import com.flink.model.CallGenerator;

public class CareTicketProcessor {
	String BOOTSTRAP_SERVER=null;
	
	public CareTicketProcessor() {}
	
	public CareTicketProcessor(String bootstrap_server) {
		this.BOOTSTRAP_SERVER=bootstrap_server;
		
	}
	
	
	public void ticketCount()  throws Exception{
		String care_ticket_topic = "care_ticket_topic";
		String care_output_topic="care_output_topic";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Producer<String> dataGenerator = new Producer<String>(BOOTSTRAP_SERVER, StringSerializer.class.getName(),care_ticket_topic);
		Producer<String> targetProducer = new Producer<String>(BOOTSTRAP_SERVER, StringSerializer.class.getName(),care_output_topic);
		
		
		
		
		Properties props = new Properties();
		props.put("bootstrap.servers", BOOTSTRAP_SERVER);
		props.put("client.id", "flink-example1");

		
		// Reading data directly as <Key, Value> from Kafka. Write an inner class
		// containing key, value
		// and use it to deserialise Kafka record.
		// Reference =>
		// https://stackoverflow.com/questions/53324676/how-to-use-flinkkafkaconsumer-to-parse-key-separately-k-v-instead-of-t
		FlinkKafkaConsumer<TicketKafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(care_ticket_topic, new CareTicketRecord(), props);

		kafkaConsumer.setStartFromLatest();

		// create a stream to ingest data from Kafka as a custom class with explicit
		// key/value
		DataStream<TicketKafkaRecord> stream = env.addSource(kafkaConsumer);

		// supports timewindow without group by key
		
		stream.timeWindowAll(Time.seconds(5)).reduce(new ReduceFunction<TicketKafkaRecord>() {
			TicketKafkaRecord result = new TicketKafkaRecord();
			int counter=1;
			@Override
			public TicketKafkaRecord reduce(TicketKafkaRecord record1, TicketKafkaRecord record2) throws Exception {
				counter ++;
				//result.setCount(record1.getCount()+record2.getCount());
				result.count=counter;
				result.areaCode = record1.areaCode;
				//result.value = record1.value + record2.value;
				return result;
			}
		}).addSink((SinkFunction<TicketKafkaRecord>) targetProducer); // immediate printing to console

		// .keyBy( (KeySelector<KafkaRecord, String>) KafkaRecord::getKey )
		// .timeWindow(Time.seconds(5))

		// produce a number as string every second
		new TicketGenerator(dataGenerator, care_ticket_topic).start();

		// for visual topology of the pipeline. Paste the below output in
		// https://flink.apache.org/visualizer/
	 System.out.println(env.getExecutionPlan());
		env.execute();
	}
}
