/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.metrics;

import io.prometheus.jmx.JmxCollector;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

import javax.management.MalformedObjectNameException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Allow to collect JMX metrics exposing them in the Prometheus format.
 */
public class JmxCollectorRegistry {
    private final PrometheusRegistry registry;
    private final PrometheusTextFormatWriter textFormatter;

    /**
     * Constructor.
     *
     * @param yamlConfig YAML configuration string with metrics filtering rules
     * @throws MalformedObjectNameException Throws MalformedObjectNameException
     */
    public JmxCollectorRegistry(String yamlConfig) throws MalformedObjectNameException {
        // note that Prometheus default registry is a singleton, so it is shared with JmxCollector
        this(new JmxCollector(yamlConfig), PrometheusRegistry.defaultRegistry, new PrometheusTextFormatWriter(true));
    }

    /**
     * Constructor.
     *
     * @param jmxCollector JMX collector registry
     * @param registry Prometheus collector registry
     * @param textFormatter Prometheus text formatter
     */
    /* test */ JmxCollectorRegistry(JmxCollector jmxCollector,
                                    PrometheusRegistry registry,
                                    PrometheusTextFormatWriter textFormatter) {
        jmxCollector.register();
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
