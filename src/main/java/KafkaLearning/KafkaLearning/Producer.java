package KafkaLearning.KafkaLearning;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

	private String topic = "my-java-topic";
    private final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094,localhost:9095";

    public void setTopic(String topic) {
		this.topic = topic;
	}
    
    public void produce() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i<4; i++) {
                String key = Integer.toString(i);
                String message = "JAVA Message - Najat: " + Integer.toString(i);

                producer.send(new ProducerRecord<String, String>(topic, key, message));

                System.out.println("sent msg " + key);
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("Could not start producer: " + e);
        }
    }
}
