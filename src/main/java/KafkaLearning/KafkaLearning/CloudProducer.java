package KafkaLearning.KafkaLearning;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class CloudProducer {
	public static Properties readConfig(final String configFile) throws IOException {
	    // reads the client configuration from client.properties
	    // and returns it as a Properties object
		File f = new File(configFile);
		System.out.println("File exists: " + f.exists() + " File abs:" + f.isAbsolute());
		
	    if (!Files.exists(Paths.get(configFile)) && !f.isAbsolute()) {
	      throw new IOException(configFile + " not found.");
	    }

	    final Properties config = new Properties();
	    try (InputStream inputStream = new FileInputStream(configFile)) {
	      config.load(inputStream);
	    }

	    return config;
	 }
	
	public static Properties readConfig2(final String configFile) throws IOException {
		Properties prop = new Properties();
		InputStream stream = CloudProducer.class.getResourceAsStream(configFile);
		prop.load(stream);
		return prop;
	  }
	
	public void produce() throws IOException {
		String topic = "FirstJavaTopic";
		final Properties config = readConfig("/home/najat/Downloads/KafkaLearning/src/main/java/KafkaLearning/KafkaLearning/config.properties");   //("config.properties");

		// sets the message serializers
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// creates a new producer instance and sends a sample message to the topic
		String key = "key";
		String value = "value";
		org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(config);
		producer.send(new ProducerRecord<>(topic, key, value));
		System.out.println(
		  String.format(
		    "Produced message to Confluent topic %s: key = %s value = %s", topic, key, value
		  )
		);

		// closes the producer connection
		producer.close();
	}
}
