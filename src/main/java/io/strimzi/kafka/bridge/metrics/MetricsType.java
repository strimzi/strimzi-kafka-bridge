/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.metrics;

/**
 * Metrics type.
 */
public enum MetricsType {
    /** JMX Prometheus Exporter.  */
    JMX_EXPORTER("jmxPrometheusExporter"),
    
    /** Strimzi Metrics Reporter. */
    STRIMZI_REPORTER("strimziMetricsReporter");

    private final String text;

    /**
     * @param text
     */
    MetricsType(final String text) {
        this.text = text;
    }
    
    @Override
    public String toString() {
        return text;
    }
}
