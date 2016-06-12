package com.kaf;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSpeaker {

	protected static Logger log = LoggerFactory.getLogger(KafkaSpeaker.class);

	private final KafkaProducer<String, String> producer;
	private final List<String> topics;
	private final int id;
	
	public KafkaSpeaker(int id, Properties prop,
			List<String> topics) {
		this.id = id;
		this.topics = topics;
		Properties props = new Properties();
		props.put("bootstrap.servers", prop.getProperty("producer.url"));
		props.put("client.id", prop.getProperty("producer.client.id"));
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		this.producer = new KafkaProducer<>(props);
		log.info(Thread.currentThread().getId()+", Initialized Kafka Speaker with properties: "+props.toString());
	}

	public void send() {
		log.info("Thread:"+Thread.currentThread().getId()+", Publishing to topic: "+topics);
		/*
		 * Initialize this and send 
		 * 
		 */
		try {
			for (int i = 0; i < 10; i++) {
				// send lots of messages
				for(String topic : topics)
				producer.send(new ProducerRecord<String, String>(
							topic,
							String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)
							));
			}
		} catch (Exception e) {
			log.error(e.getMessage(),e);
		} finally {
			producer.close();
		}
	}

}
