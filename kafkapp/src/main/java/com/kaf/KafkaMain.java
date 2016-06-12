package com.kaf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kaf.utility.Utility;


/**
 * Kafka Main Interface 
 *
 */
public class KafkaMain {
	
	protected static Logger log = LoggerFactory.getLogger(KafkaMain.class);

	static String usage = "\nUsage: Change directory to your project home folder"
			+ "\n\t cd target"
			+ "java -cp . -jar kafkapp.jar -resources ./ "
			+ "\n\t "
			+ "java -cp . -jar target/kafkapp.jar -resources src/main/resources "
			+ "\n";

	static Map<String,String> parameters = new HashMap<>();
	String resourcePath = "";
	Properties prop = new Properties();


	/**
	 * Constructor
	 */
	public KafkaMain(String resource) {
		super();
		this.resourcePath = resource;
		String configKafkaPath = "kaflis.properties";
		String configWmaPath = "wctr.properties";
		try {
			configKafkaPath = resourcePath+File.separator+configKafkaPath;
			log.info("Read configuration: "+configKafkaPath);
			InputStream inputKaf = new FileInputStream(configKafkaPath);
			prop.load(inputKaf);
			configWmaPath = resourcePath+File.separator+configWmaPath;
			log.info("Read configuration: "+configWmaPath);
			InputStream inputWma = new FileInputStream(configWmaPath);
			prop.load(inputWma);
		} catch (IOException e) {
			log.error(e.getMessage(),e);
		}
	}

	/**
	 * 
	 * MAIN
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		//log.info("-----------------------------INITIALIZING-------------------------------------------");
		parseParameters(args);
		String resourcePath = "";
		//Configure Parameters
		if(parameters.size()>0)
		if(parameters.containsKey("-resources")){
			log.info("Parameters: "+parameters);
			resourcePath = parameters.get("-resources");
			log.info("Using resource path: "+resourcePath);	
		}
		//Instantiate this main component
		KafkaMain main = new KafkaMain(resourcePath);
		log.info("Execute Class "+main.getClass().getSimpleName()+" with full package name "+main.getClass().getName());
		main.listen();
		//System.exit(1);
	}

	/**
	 * Parse parameters
	 * 
	 * @param args
	 */
	public static void parseParameters(String[] args){
		if (args.length > 0) {
			for(int i = 0; i < args.length; i++) {
				log.debug(args[i] + " ", false);
			}
			if(args.length == 1) {
				System.out.println(usage);
				log.error(usage);
				System.exit(1);
			}else{
				for(int a=0; a<args.length; a++){
				if((a+1)<args.length){
					try {
						String key = args[a];
						String value = args[++a];
						parameters.put(key,value);
					} catch (Exception e) {
						log.error("Unable to parse parameters");
					}
				}else{
					log.error("Unable to parse parameters "+args[a]);
				}
				}
			}
		}else
			log.error("No Arguments supplied. "+usage);
	}

	/**
	 * Execute the consumer 
	 */
	public void listen(){
		log.info("-----------------------------CONSUME-------------------------------------------");
		try {
			if(prop.isEmpty())
				throw new Exception("Unable to read configuration at "+resourcePath);
			//Initialize the Listener Thread
			int numConsumers = Integer.parseInt(prop.getProperty("consumer.count"));
			String topic = prop.getProperty("consumer.topic");
			List<String> topics = Arrays.asList(topic);
			final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
			final List<KafkaListener> consumers = new ArrayList<>();
			for (int i = 0; i < numConsumers; i++) {
				KafkaListener consumer = new KafkaListener(i, prop, topics);
				consumers.add(consumer);
				executor.submit(consumer);
			}
			//Add a shutdown hook
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					for (KafkaListener consumer : consumers) {
						consumer.shutdown();
					}
					log.info("Shutdown Kafka Listener");
					executor.shutdown();
					try {
						executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
					} catch (InterruptedException e) {
						log.error(e.getMessage(),e);
					}
				}
			});
		} catch (Exception e) {
			log.error(e.getMessage(),e);
		}
		log.info("------------------------------------------------------------------------");
	}

	/**
	 * Execute the producer
	 */
	public void speak(){
		log.info("-----------------------------PRODUCE-------------------------------------------");
		try {
			if(prop.isEmpty())
				throw new Exception("Unable to read configuration at "+resourcePath);
			String topic = prop.getProperty("producer.topic");
			List<String> topics = Arrays.asList(topic);
			KafkaSpeaker producer = new KafkaSpeaker(1, prop, topics);
			producer.send();
		} catch (Exception e) {
			log.error(e.getMessage(),e);
		}
		log.info("------------------------------------------------------------------------");	
	}
	

}
