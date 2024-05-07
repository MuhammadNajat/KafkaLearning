package KafkaLearning.KafkaLearning;

import java.lang.System;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.time.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

public class CloudConsumer {
  public static Properties readConfig(final String configFile) throws IOException {
    // reads the client configuration from client.properties
    // and returns it as a Properties object
	File f = new File("/" + configFile);
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

  public void consume() throws IOException {
	  String topic = "FirstJavaTopic";
	  final Properties config = readConfig("/home/najat/Downloads/KafkaLearning/src/main/java/KafkaLearning/KafkaLearning/config.properties");   //("config.properties");
	  // sets the group ID, offset and message deserializers
	  config.put(ConsumerConfig.GROUP_ID_CONFIG, "java-group-1");
	  config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	  config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	  config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

	  // creates a new consumer instance and subscribes to messages from the topic
	  KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
	  consumer.subscribe(Arrays.asList(topic));

	  while (true) {
	    // polls the consumer for new messages and prints them
	    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
	    for (ConsumerRecord<String, String> record : records) {
	      System.out.println(
	        String.format(
	          "Consumed message from Confluent topic %s: key = %s value = %s", topic, record.key(), record.value()
	        )
	      );
	    }
	  }
  }
}