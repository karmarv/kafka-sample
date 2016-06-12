package com.kaf;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Rahul Vishwakarma
 *
 */
public class KafInterfaceTest {

	protected static Logger log = LoggerFactory.getLogger(KafInterfaceTest.class);
	static String usage = "Usage: java -cp .  -jar kaflis.jar ";	
	static Map<String,String> parameters = new HashMap<>();
	static String resourcePath = "C:\\Users\\vishwaka\\git\\kafk\\kafkapp\\src\\main\\resources";
	
	/**
	 * 
	 * MAIN TEST
	 * 
	 * @param args
	 * @throws IllegalAccessException 
	 * @throws IllegalArgumentException 
	 */
	public static void main(String[] args) throws IllegalArgumentException, IllegalAccessException {
		testInitKafLis();
		//testInitKafProduce();
	}

	/**
	 * Consumer test
	 */
	public static void testInitKafLis(){
		log.info("-----------------------------TEST LISTENER-------------------------------------------");
		try {
			
			KafkaMain kafLisMain = new KafkaMain(resourcePath);
			
			log.info("Execute Class "+kafLisMain.getClass().getSimpleName()+" with full package name "+kafLisMain.getClass().getName());
			kafLisMain.listen();
			log.info("------------------------------------------------------------------------");
		} catch (Exception e) {
			log.error(e.getMessage(),e);
		}
		
	}

	/**
	 * Producer Test
	 */
	public static void testInitKafProduce(){
		log.info("-----------------------------TEST PRODUCER-------------------------------------------");
		try {
			KafkaMain kafLisMain = new KafkaMain(resourcePath);
			log.info("Execute Class "+kafLisMain.getClass().getSimpleName()+" with full package name "+kafLisMain.getClass().getName());
			kafLisMain.speak();
			log.info("------------------------------------------------------------------------");
		} catch (Exception e) {
			log.error(e.getMessage(),e);
		}
		
	}
	

}
