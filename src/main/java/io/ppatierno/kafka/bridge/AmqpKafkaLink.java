package io.ppatierno.kafka.bridge;

import io.vertx.proton.ProtonLink;

public interface AmqpKafkaLink {

	void handle(ProtonLink<?> link);
}
