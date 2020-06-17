/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;

/**
 * Used for scraping and reporting metrics in Prometheus format
 */
public class MetricsReporter {

    private final JmxCollectorRegistry jmxCollectorRegistry;
    private final MeterRegistry meterRegistry;

    /**
     * Constructor
     *
     * @param jmxCollectorRegistry JmxCollectorRegistry instance for scraping metrics from JMX endpoints
     * @param meterRegistry MeterRegistry instance for scraping metrics exposed through Vert.x
     */
    public MetricsReporter(JmxCollectorRegistry jmxCollectorRegistry, MeterRegistry meterRegistry) {
        this.jmxCollectorRegistry = jmxCollectorRegistry;
        this.meterRegistry = meterRegistry;
    }

    /**
     * @return JmxCollectorRegistry instance for scraping metrics from JMX endpoints
     */
    public JmxCollectorRegistry getJmxCollectorRegistry() {
        return jmxCollectorRegistry;
    }

    /**
     * @return MeterRegistry instance for scraping metrics exposed through Vert.x
     */
    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    /**
     * Scrape metrics on the provided registries returning them in the Prometheus format
     *
     * @return metrics in Prometheus format as String
     */
    public String scrape() {
        StringBuilder sb = new StringBuilder();
        if (jmxCollectorRegistry != null) {
            sb.append(jmxCollectorRegistry.scrape());
        }
        if (meterRegistry instanceof PrometheusMeterRegistry) {
            sb.append(((PrometheusMeterRegistry) meterRegistry).scrape());
        }
        return sb.toString();
    }
}
