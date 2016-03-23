package io.ppatierno.kafka.bridge;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

/**
 * Class in charge for handling AMQP-Kafka bridge configuration
 * 
 * @author ppatierno
 */
public class AmqpKafkaConfig {
	
	// Keys for accessing fields in the configuration properties file
	
	// Apache Kafka producer
	public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
	public static final String KEY_SERIALIZER = "key.serializer";
	public static final String VALUE_SERIALIZER = "value.serializer";
	// AMQP receiver
	public static final String FLOW_CREDIT = "flow.credit";
	
	// default configuration values
	
	private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
	private static final String DEFAULT_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	private static final String DEFAULT_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
	
	private static final int DEFAULT_FLOW_CREDIT = 10;
	
	private static Properties props;

	/**
	 * Load bridge configuration from properties file
	 * 
	 * @param path	configuration file path
	 * @return		load result
	 */
	public static boolean load(String path) {
		
		File configFile = new File(path);
		try {
			FileReader reader = new FileReader(configFile);
			
			// check properties collection and clear it if already filled
			if (props == null)
				props = new Properties();
			else
				props.clear();
			
			props.load(reader);
			return true;
			
		} catch (Exception e) {
			
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * Load default bridge configuration
	 * 
	 * @return		load result
	 */
	public static boolean loadDefault() {
		
		// check properties collection and clear it if already filled
		if (props == null)
			props = new Properties();
		else
			props.clear();
		
		props.put(AmqpKafkaConfig.BOOTSTRAP_SERVERS, AmqpKafkaConfig.DEFAULT_BOOTSTRAP_SERVERS);
		props.put(AmqpKafkaConfig.KEY_SERIALIZER, AmqpKafkaConfig.DEFAULT_KEY_SERIALIZER);
		props.put(AmqpKafkaConfig.VALUE_SERIALIZER, AmqpKafkaConfig.DEFAULT_VALUE_SERIALIZER);
		props.put(AmqpKafkaConfig.FLOW_CREDIT, String.valueOf(AmqpKafkaConfig.DEFAULT_FLOW_CREDIT));
		
		return true;
	}
	
	/**
	 * Bootstrap servers to which Kafka Producer connect 
	 * @return
	 */
	public static String getBootstrapServers() {
		return props.getProperty(AmqpKafkaConfig.BOOTSTRAP_SERVERS);
	}
	
	/**
	 * Serialzer used for the key by the Kafka Producer
	 * @return
	 */
	public static String getKeySerializer() {
		return props.getProperty(AmqpKafkaConfig.KEY_SERIALIZER);
	}
	
	/**
	 * Serializer used for the value by the Kafka Producer
	 * @return
	 */
	public static String getValueSerializer() {
		return props.getProperty(AmqpKafkaConfig.VALUE_SERIALIZER);
	}
	
	/**
	 * Link credit for flow control on the AMQP receiver side
	 * @return
	 */
	public static int getFlowCredit() {
		return Integer.parseInt(props.getProperty(AmqpKafkaConfig.FLOW_CREDIT));
	}
}
