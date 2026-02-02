/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.metrics;

/**
 * Metrics type.
 */
public enum MetricsType {
    /** Prometheus JMX Exporter. */
    JMX_EXPORTER("jmxPrometheusExporter"),
    
    /** Strimzi Metrics Reporter. */
    STRIMZI_REPORTER("strimziMetricsReporter");

    private final String text;
    
    MetricsType(final String text) {
        this.text = text;
    }
    
    @Override
    public String toString() {
        return text;
    }

    /**
     * Get the metrics type from the given text.
     *
     * @param text Text.
     * @return Get type from text.
     */
    public static MetricsType fromString(String text) {
        for (MetricsType t : MetricsType.values()) {
            if (t.text.equalsIgnoreCase(text)) {
                return t;
            }
        }
        throw new IllegalArgumentException("Metrics type not found: " + text);
    }
}
