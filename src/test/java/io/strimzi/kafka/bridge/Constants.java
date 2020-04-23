package io.strimzi.kafka.bridge;

public interface Constants {

    /**
     * Tag for http bridge tests, which are triggered for each push/pr/merge on travis-ci
     */
    String HTTP_BRIDGE = "httpbridge";

    /**
     * Tag for amqp bridge tests, which are triggered for each push/pr/merge on travis-ci
     */
    String AMQP_BRIDGE = "amqpbridge";
}
