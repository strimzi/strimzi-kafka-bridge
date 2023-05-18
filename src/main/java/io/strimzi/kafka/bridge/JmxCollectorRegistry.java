/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import io.prometheus.jmx.JmxCollector;
import io.quarkus.runtime.Startup;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.management.MalformedObjectNameException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

/**
 * Allow to collect JMX metrics exposing them in the Prometheus format
 */
@Startup
public class JmxCollectorRegistry {

    private CollectorRegistry collectorRegistry;

    @PostConstruct
    public void init() throws MalformedObjectNameException, IOException {
        InputStream is = getClass().getClassLoader().getResourceAsStream("jmx_metrics_config.yaml");
        if (is == null) {
            // this should not happen because the JMX metrics configuration is baked into the jar
            throw new RuntimeException("JMX metrics configuration not found");
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String yaml = reader
                    .lines()
                    .collect(Collectors.joining("\n"));
            new JmxCollector(yaml).register();
            collectorRegistry = CollectorRegistry.defaultRegistry;
        }
    }

    @PreDestroy
    public void destroy() {
        collectorRegistry.clear();
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
