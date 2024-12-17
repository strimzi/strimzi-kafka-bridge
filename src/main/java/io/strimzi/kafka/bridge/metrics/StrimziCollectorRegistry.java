/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.metrics;

import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Allow to collect Strimzi Reporter metrics exposing them in the Prometheus format.
 */
public class StrimziCollectorRegistry {
    private final PrometheusRegistry registry;
    private final PrometheusTextFormatWriter textFormatter;

    /**
     * Constructor.
     */
    public StrimziCollectorRegistry() {
        // note that Prometheus default registry is a singleton, so it is shared with Strimzi Metrics Reporter
        this(PrometheusRegistry.defaultRegistry, new PrometheusTextFormatWriter(true));
    }

    /**
     * Constructor.
     *
     * @param registry Prometheus collector registry
     * @param textFormatter Prometheus text formatter
     */
    /* test */ StrimziCollectorRegistry(PrometheusRegistry registry,
                                        PrometheusTextFormatWriter textFormatter) {
        this.registry = registry;
        this.textFormatter = textFormatter;
    }

    /**
     * @return Content that should be included in the response body for 
     * an endpoint designated for Prometheus to scrape from.
     */
    public String scrape() {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            textFormatter.write(stream, registry.scrape());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return stream.toString(StandardCharsets.UTF_8);
    }
}
