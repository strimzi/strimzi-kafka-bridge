package io.ppatierno.kafka.bridge;

import io.vertx.proton.ProtonLink;
import io.vertx.proton.ProtonSender;

/**
 * Class in charge for handling incoming AMQP traffic
 * from senders and bridging into Apache Kafka
 * 
 * @author ppatierno
 */
public class AmqpKafkaProducer implements AmqpKafkaLink {

	@Override
	public void handle(ProtonLink<?> link) {
		
		if (!(link instanceof ProtonSender)) {
			throw new IllegalArgumentException("This Proton link must be a sender");
		}
	}

}
