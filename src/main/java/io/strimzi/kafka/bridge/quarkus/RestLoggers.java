/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.quarkus;

import io.quarkus.runtime.Startup;
import io.strimzi.kafka.bridge.http.HttpOpenApiOperations;
import org.jboss.logging.Logger;

import javax.annotation.PostConstruct;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Hosts a collection of loggers, one for each HTTP OpenAPI operation
 * in order to be able to set the corresponding logging level via Quarkus "categories"
 */
@Startup
public class RestLoggers {

    private final static String LOGGER_NAME_PREFIX = "http.openapi.operation.";

    private ConcurrentMap<String, Logger> loggers;

    @PostConstruct
    public void init() {
        loggers = new ConcurrentHashMap<>();
        for (HttpOpenApiOperations operation : HttpOpenApiOperations.values()) {
            loggers.put(operation.toString(), Logger.getLogger(LOGGER_NAME_PREFIX + operation));
        }
    }

    /**
     * Return the logger corresponding to an HTTP OpenAPI operation
     *
     * @param name HTTP OpenAPI operation name
     * @return logger instance
     */
    public Logger get(String name) {
        return name != null ? loggers.get(name) : null;
    }
}
