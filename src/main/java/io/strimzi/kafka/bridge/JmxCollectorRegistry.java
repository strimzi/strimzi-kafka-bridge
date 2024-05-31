/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.prometheus.jmx.JmxCollector;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

import javax.management.MalformedObjectNameException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Allow to collect JMX metrics exposing them in the Prometheus format
 */
public class JmxCollectorRegistry {
    private final PrometheusRegistry collectorRegistry;
    private final PrometheusTextFormatWriter textFormatter = new PrometheusTextFormatWriter(true);

    /**
     * Constructor
     *
     * @param yamlConfig YAML configuration string with metrics filtering rules
     * @throws MalformedObjectNameException Throws MalformedObjectNameException
     */
    public JmxCollectorRegistry(String yamlConfig) throws MalformedObjectNameException {
        new JmxCollector(yamlConfig).register();
        collectorRegistry = PrometheusRegistry.defaultRegistry;
    }

    /**
     * Constructor
     *
     * @param yamlFileConfig file containing the YAML configuration with metrics filtering rules
     * @throws MalformedObjectNameException Throws MalformedObjectNameException
     * @throws IOException Throws IOException
     */
    public JmxCollectorRegistry(File yamlFileConfig) throws MalformedObjectNameException, IOException {
        new JmxCollector(yamlFileConfig).register();
        collectorRegistry = PrometheusRegistry.defaultRegistry;
    }

    /**
     * @return Content that should be included in the response body for an endpoint designated for
     * Prometheus to scrape from.
     */
    public String scrape() {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            textFormatter.write(stream, collectorRegistry.scrape());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return stream.toString(StandardCharsets.UTF_8);
    }
}
