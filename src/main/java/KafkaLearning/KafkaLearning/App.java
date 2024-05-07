package KafkaLearning.KafkaLearning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class App 
{
    public static void main( String[] args ) throws InterruptedException, IOException
    {	
    	testKafkaLocally();
    	
    	//testKafkaOnCloud();
    }
    
    public static void testKafkaOnCloud() throws IOException {
    	CloudProducer cloudProducer = new CloudProducer();
    	cloudProducer.produce();
    	
    	CloudConsumer cloudConsumer = new CloudConsumer();
    	cloudConsumer.consume();
    }
    
    
    public static void testKafkaLocally() throws InterruptedException {
    	Runnable producerRunnable = new Runnable() {
			@Override
			public void run() {
				Producer producer = new Producer();
				producer.setTopic("myDiary.01");
				producer.produce();
			}
		};
		
		Thread producerThread = new Thread(producerRunnable);
		
		producerThread.start();
		producerThread.join();
    	

		Runnable consumerRunnable = new Runnable() {
			@Override
			public void run() {
				Consumer consumer = new Consumer();
				String groupID = ".01";
				consumer.setGroupID(groupID);
				consumer.setTopic("myDiary.01");
				consumer.consume();
			}
		};		
		
		Thread consumerThread = new Thread(consumerRunnable);
	
		consumerThread.start();
		consumerThread.join();
    }
}
