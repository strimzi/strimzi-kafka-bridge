package io.strimzi.kafka.bridge;

/**
 * Represents a component which exposes information about its health
 */
public interface HealthCheckable {

    /**
     * @return if it's healthy answering to requests
     */
    boolean isHealthy();

    /**
     * @return if it's ready to start answering to requests
     */
    boolean isReady();
}