/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.micrometer.prometheus.PrometheusNamingConvention;
import io.vertx.micrometer.backends.BackendRegistries;

/**
 * Abstract class for collecting and exposing metrics.
 */
public abstract class MetricsCollector {
    private final PrometheusMeterRegistry vertxRegistry;
    
    MetricsCollector() {
        this.vertxRegistry = (PrometheusMeterRegistry) BackendRegistries.getDefaultNow();
        if (vertxRegistry != null) {
            // replace the default Prometheus naming convention
            this.vertxRegistry.config().namingConvention(new MetricsNamingConvention());
        }
    }

    private static class MetricsNamingConvention extends PrometheusNamingConvention {
        @Override
        public String name(String name, Meter.Type type, String baseUnit) {
            String metricName = name.startsWith("vertx.") ? name.replace("vertx.", "strimzi.bridge.") : name;
            return super.name(metricName, type, baseUnit);
        }
    }

    /**
     * @return Registry instance for scraping Vertx metrics.
     *         This is null if metrics are not enabled in the VertxOptions instance.
     */
    public MeterRegistry getVertxRegistry() {
        return vertxRegistry;
    }

    /**
     * Scrape all, including Vertx metrics.
     * 
     * @return Raw metrics in Prometheus format.
     */
    public String scrape() {
        StringBuilder sb = new StringBuilder();
        sb.append(doScrape());
        if (vertxRegistry != null) {
            sb.append(vertxRegistry.scrape());
        }
        return sb.toString();
    }

    /**
     * @return Raw metrics in Prometheus format.
     */
    abstract String doScrape();
}
