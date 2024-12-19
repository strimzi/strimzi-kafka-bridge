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
 * Used for scraping and reporting metrics in Prometheus format.
 */
public class MetricsReporter {
    private final JmxCollectorRegistry jmxRegistry;
    private final StrimziCollectorRegistry strimziRegistry;
    private final PrometheusMeterRegistry vertxRegistry;

    /**
     * Constructor.
     *
     * @param jmxRegistry Registry instance for scraping metrics from JMX endpoints
     */
    public MetricsReporter(JmxCollectorRegistry jmxRegistry) {
        // default registry is null if metrics are not enabled in the VertxOptions instance
        this(jmxRegistry, null, (PrometheusMeterRegistry) BackendRegistries.getDefaultNow());
    }

    /**
     * Constructor.
     *
     * @param strimziRegistry Registry instance for scraping metrics from Strimzi Metrics Reporter
     */
    public MetricsReporter(StrimziCollectorRegistry strimziRegistry) {
        // default registry is null if metrics are not enabled in the VertxOptions instance
        this(null, strimziRegistry, (PrometheusMeterRegistry) BackendRegistries.getDefaultNow());
    }

    /**
     * Constructor.
     *
     * @param jmxRegistry Registry instance for scraping metrics from JMX endpoints
     * @param strimziRegistry Registry instance for scraping metrics from Strimzi Metrics Reporter
     * @param vertxRegistry Registry instance for scraping metrics exposed through Vert.x
     */
    public MetricsReporter(JmxCollectorRegistry jmxRegistry,
                           StrimziCollectorRegistry strimziRegistry,
                           PrometheusMeterRegistry vertxRegistry) {
        this.jmxRegistry = jmxRegistry;
        this.strimziRegistry = strimziRegistry;
        this.vertxRegistry = vertxRegistry;
        if (vertxRegistry != null) {
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
     * @return Registry instance for scraping metrics exposed through Vert.x.
     *         This is null if metrics are not enabled in the VertxOptions instance.
     */
    public MeterRegistry getVertxRegistry() {
        return vertxRegistry;
    }

    /**
     * Scrape metrics on the provided registries returning them in the Prometheus format
     *
     * @return metrics in Prometheus format as String
     */
    public String scrape() {
        StringBuilder sb = new StringBuilder();
        if (jmxRegistry != null) {
            sb.append(jmxRegistry.scrape());
        }
        if (strimziRegistry != null) {
            sb.append(strimziRegistry.scrape());
        }
        if (vertxRegistry != null) {
            sb.append(vertxRegistry.scrape());
        }
        return sb.toString();
    }
}
