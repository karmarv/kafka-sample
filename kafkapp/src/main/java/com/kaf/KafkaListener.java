/**
 * 
 */
package com.kaf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * @author vishwaka
 *
 * Ref: http://www.confluent.io/blog/tutorial-getting-started-with-the-new-apache-kafka-0.9-consumer-client
 */
public class KafkaListener implements Runnable{

	protected static Logger log = LoggerFactory.getLogger(KafkaListener.class);

	private final KafkaConsumer<String, String> consumer;
	private final List<String> topics;
	private final int id;
	private final int timeOut;
	
	/**
	 * Constructor with properties
	 */
	public KafkaListener(int id, Properties prop,
						List<String> topics) {
		this.id = id;
		this.topics = topics;
		
		Properties props = new Properties();
		props.put("bootstrap.servers", prop.getProperty("consumer.url"));
		String groupId = prop.getProperty("consumer.group.id");
		props.put("group.id", groupId);	
		props.put("client.id", prop.getProperty("consumer.client.id"));
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "2000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		this.consumer = new KafkaConsumer<>(props);
		this.timeOut = Integer.parseInt( prop.getProperty("consumer.poll.timeout"));
		log.info(Thread.currentThread().getId()+", Initialized Kafka Listener with properties: "+props.toString());
	}


	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		try {
			consumer.subscribe(topics);
			log.info("Thread:"+Thread.currentThread().getId()+", Subscribed topics: "+topics);
			/*
			 * Initialize this like a server which listens on the topic until eternity.
			 * 
			 */
			int timeouts = 0;
			while (true) {
				System.out.print(".");
				ConsumerRecords<String, String> records = consumer.poll(this.timeOut);
				if(records == null){
					System.out.print("-");
					timeouts++;
				}else if (records.count() == 0) {
					System.out.print(".");
					timeouts++;
				} else {
					System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
					timeouts = 0;
				}
				for (ConsumerRecord<String, String> record : records) {
					Map<String, Object> data = new HashMap<>();
					data.put("partition", record.partition());
					data.put("offset", record.offset());
					data.put("value", record.value());
					log.info(record.topic()+" - "+this.id + ", Data-Length: " + data.get("value"));
				}
			}
		} catch (WakeupException e) {
			log.error("Thread:"+Thread.currentThread().getId()+", Shutdown Initiated. "+e.getMessage(),e);
		} finally {
			consumer.close();
			log.info("Thread:"+Thread.currentThread().getId()+", End");
		}
	}

	/**
	 * Shutdown
	 */
	public void shutdown() {
		 consumer.wakeup();
	}

}
