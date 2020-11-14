# spp-assignment-telephone

This is an example of Kafka + Flink streaminng aggregation. 

# Setup
    1) Install Kafka - If you are using mac, you can brew install kafka 
    2) Install Fllink - If you are using mac, you can brew install flink
    3) Start Kafka & Flink
    4) Create topics in Kafka - call_topic, care_ticket_topic,call_output_topic,care_output_topic,
    5) Navigate to- spp-assignment-telephone folder and run mvn clean package . Make sure maven is installed.
    6) Navigate to Flink home folder and run bin/flink run -c com.flink.MainAggregator {path_to_directoy}/spp_assign_telephone/target/spp_assign_telephone_new.jar
    7) Two Generators - Call and Care Ticket generators will produce requests on the input kafka topics, that are consumed by Flink job.
    8) Output of Flink job is directed to respective Kafka topic. 
