/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An HTTP server exposing endpoints for health and metrics used when the HTTP bridge is not enabled
 */
public class EmbeddedHttpServer {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedHttpServer.class);

    private final HealthChecker healthChecker;
    private final MeterRegistry meterRegistry;
    private final JmxCollectorRegistry jmxCollectorRegistry;
    private final Vertx vertx;
    private final int port;

    /**
     * Constructor
     *
     * @param vertx Vert.x instance
     * @param healthChecker HealthChecker instance for checking health of enabled bridges
     * @param meterRegistry registry for scraping metrics
     * @param jmxCollectorRegistry registry for scraping JMX metrics
     * @param port port on which listening requests
     */
    public EmbeddedHttpServer(Vertx vertx, HealthChecker healthChecker, MeterRegistry meterRegistry, JmxCollectorRegistry jmxCollectorRegistry, int port) {
        this.vertx = vertx;
        this.healthChecker = healthChecker;
        this.meterRegistry = meterRegistry;
        this.jmxCollectorRegistry = jmxCollectorRegistry;
        this.port = port;
    }

    /**
     * Create and start the HTTP server
     */
    public void start() {
        vertx.createHttpServer()
                .requestHandler(request -> {
                    HttpResponseStatus httpResponseStatus;
                    if (request.path().equals("/healthy")) {
                        httpResponseStatus = healthChecker.isAlive() ? HttpResponseStatus.OK : HttpResponseStatus.NOT_FOUND;
                        request.response().setStatusCode(httpResponseStatus.code()).end();
                    } else if (request.path().equals("/ready")) {
                        httpResponseStatus = healthChecker.isReady() ? HttpResponseStatus.OK : HttpResponseStatus.NOT_FOUND;
                        request.response().setStatusCode(httpResponseStatus.code()).end();
                    } else if (request.path().equals("/metrics")) {
                        PrometheusMeterRegistry prometheusMeterRegistry = (PrometheusMeterRegistry) meterRegistry;

                        StringBuilder sb = new StringBuilder();
                        sb.append(jmxCollectorRegistry.scrape());
                        sb.append(prometheusMeterRegistry.scrape());

                        request.response().setStatusCode(HttpResponseStatus.OK.code()).end(sb.toString());
                    } else {
                        request.response().setStatusCode(HttpResponseStatus.NOT_FOUND.code()).end();
                    }
                })
                .listen(port, done -> {
                    if (done.succeeded()) {
                        log.info("Embedded HTTP server started, listening on port {}", port);
                    } else {
                        log.error("Failed to start the embedded HTTP server", done.cause());
                    }
                });
    }
}
