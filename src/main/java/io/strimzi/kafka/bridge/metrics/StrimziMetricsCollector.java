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
 * Collect and scrape Strimzi Reporter metrics in Prometheus format.
 */
public class StrimziMetricsCollector extends MetricsCollector {
    private final PrometheusRegistry registry;
    private final PrometheusTextFormatWriter textFormatter;

    /**
     * Constructor.
     */
    public StrimziMetricsCollector() {
        // Prometheus default registry is a singleton, so it is shared with Strimzi Metrics Reporter
        this(PrometheusRegistry.defaultRegistry, new PrometheusTextFormatWriter(true));
    }

    /**
     * Constructor.
     *
     * @param registry Prometheus collector registry
     * @param textFormatter Prometheus text formatter
     */
    /* test */ StrimziMetricsCollector(PrometheusRegistry registry,
                                       PrometheusTextFormatWriter textFormatter) {
        super();
        this.registry = registry;
        this.textFormatter = textFormatter;
    }

    @Override
    public String doScrape() {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            textFormatter.write(stream, registry.scrape());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return stream.toString(StandardCharsets.UTF_8);
    }
}
