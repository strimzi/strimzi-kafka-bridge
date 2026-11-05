/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.configuration;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for specifying a file to be copied into the Kafka broker containers in tests.
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface BrokerFile {
    /**
     * Path of the file in the test resources (classpath).
     *
     * @return the classpath resource path
     */
    String resource();

    /**
     * Target path of the file in the broker container.
     *
     * @return the path in the broker container
     */
    String path();
}
