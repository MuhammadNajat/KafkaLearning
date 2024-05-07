package KafkaLearning.KafkaLearning;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

	private String topic = "my-java-topic";
    private final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094,localhost:9095";
    private String groupID = ".01";

    public void setGroupID(String groupID) {
    	this.groupID = groupID;
    }
    
    public void setTopic(String topic) {
		this.topic = topic;
	}
    

    public void consume() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.setProperty("group.id", groupID);
        props.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("auto.offset.reset", "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                // The consumer.poll method checks and waits for any new messages to arrive for the
                // subscribed topic
                // in case there are no messages for the duration specified in the argument (1000 ms
                // in this case), it returns an empty list
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf(">>> GroupID: %s received message: %s\n", groupID, record.value());
                }
            }
        }
    }
}
