package io.ppatierno.kafka.bridge;

import io.vertx.proton.ProtonLink;
import io.vertx.proton.ProtonReceiver;

/**
 * Class in charge for reading from Apache Kafka
 * and bridging into AMQP traffic to receivers
 * 
 * @author ppatierno
 *
 */
public class AmqpKafkaConsumer implements AmqpKafkaLink {

	@Override
	public void handle(ProtonLink<?> link) {
		
		if (!(link instanceof ProtonReceiver)) {
			throw new IllegalArgumentException("This Proton link must be a receiver");
		}
		
	}

}
