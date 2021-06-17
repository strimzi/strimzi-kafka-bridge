/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import io.prometheus.jmx.JmxCollector;

import javax.management.MalformedObjectNameException;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

/**
 * Allow to collect JMX metrics exposing them in the Prometheus format
 */
public class JmxCollectorRegistry {

    private final CollectorRegistry collectorRegistry;

    /**
     * Constructor
     *
     * @param yamlConfig YAML configuration string with metrics filtering rules
     * @throws MalformedObjectNameException Throws MalformedObjectNameException
     */
    public JmxCollectorRegistry(String yamlConfig) throws MalformedObjectNameException {
        new JmxCollector(yamlConfig).register();
        collectorRegistry = CollectorRegistry.defaultRegistry;
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
        collectorRegistry = CollectorRegistry.defaultRegistry;
    }

    /**
     * @return Content that should be included in the response body for an endpoint designated for
     * Prometheus to scrape from.
     */
    public String scrape() {
        Writer writer = new StringWriter();
        try {
            TextFormat.write004(writer, collectorRegistry.metricFamilySamples());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return writer.toString();
    }
}
