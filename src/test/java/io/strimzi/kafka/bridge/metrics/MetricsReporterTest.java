/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MetricsReporterTest {
    @Test
    void shouldReturnMetrics() {
        JmxCollectorRegistry mockJmxRegistry = mock(JmxCollectorRegistry.class);
        when(mockJmxRegistry.scrape()).thenReturn("jmx_metrics\n");

        StrimziCollectorRegistry mockStrimziRegistry = mock(StrimziCollectorRegistry.class);
        when(mockStrimziRegistry.scrape()).thenReturn("strimzi_metrics\n");

        PrometheusMeterRegistry mockVertxRegistry = mock(PrometheusMeterRegistry.class);
        when(mockVertxRegistry.scrape()).thenReturn("vertx_metrics\n");
        MeterRegistry.Config meterRegistryConfig = mock(MeterRegistry.Config.class);
        when(mockVertxRegistry.config()).thenReturn(meterRegistryConfig);

        MetricsReporter metricsReporter = new MetricsReporter(
            mockJmxRegistry, mockStrimziRegistry, mockVertxRegistry);

        String result = metricsReporter.scrape();

        assertThat(result, containsString("jmx_metrics"));
        assertThat(result, containsString("strimzi_metrics"));
        assertThat(result, containsString("vertx_metrics"));

        verify(mockJmxRegistry).scrape();
        verify(mockStrimziRegistry).scrape();
        verify(mockVertxRegistry).scrape();
    }

    @Test
    void shouldReturnMetricsWithoutStrimziRegistry() {
        JmxCollectorRegistry mockJmxRegistry = mock(JmxCollectorRegistry.class);
        when(mockJmxRegistry.scrape()).thenReturn("jmx_metrics\n");

        PrometheusMeterRegistry mockVertxRegistry = mock(PrometheusMeterRegistry.class);
        when(mockVertxRegistry.scrape()).thenReturn("vertx_metrics\n");
        MeterRegistry.Config meterRegistryConfig = mock(MeterRegistry.Config.class);
        when(mockVertxRegistry.config()).thenReturn(meterRegistryConfig);

        MetricsReporter metricsReporter = new MetricsReporter(mockJmxRegistry, null, mockVertxRegistry);

        String result = metricsReporter.scrape();

        assertThat(result, containsString("jmx_metrics"));
        assertThat(result, containsString("vertx_metrics"));

        verify(mockJmxRegistry).scrape();
        verify(mockVertxRegistry).scrape();
    }

    @Test
    void shouldReturnMetricsWithoutJmxRegistry() {
        StrimziCollectorRegistry mockStrimziRegistry = mock(StrimziCollectorRegistry.class);
        when(mockStrimziRegistry.scrape()).thenReturn("strimzi_metrics\n");

        PrometheusMeterRegistry mockVertxRegistry = mock(PrometheusMeterRegistry.class);
        when(mockVertxRegistry.scrape()).thenReturn("vertx_metrics\n");
        MeterRegistry.Config meterRegistryConfig = mock(MeterRegistry.Config.class);
        when(mockVertxRegistry.config()).thenReturn(meterRegistryConfig);

        MetricsReporter metricsReporter = new MetricsReporter(null, mockStrimziRegistry, mockVertxRegistry);

        String result = metricsReporter.scrape();

        assertThat(result, containsString("strimzi_metrics"));
        assertThat(result, containsString("vertx_metrics"));

        verify(mockStrimziRegistry).scrape();
        verify(mockVertxRegistry).scrape();
    }

    @Test
    void testGetVertxRegistry() {
        StrimziCollectorRegistry mockStrimziRegistry = mock(StrimziCollectorRegistry.class);
        PrometheusMeterRegistry mockVertxRegistry = mock(PrometheusMeterRegistry.class);
        MeterRegistry.Config meterRegistryConfig = mock(MeterRegistry.Config.class);
        when(mockVertxRegistry.config()).thenReturn(meterRegistryConfig);

        MetricsReporter metricsReporter = new MetricsReporter(null, mockStrimziRegistry, mockVertxRegistry);

        MeterRegistry result = metricsReporter.getVertxRegistry();

        assertThat(result, is(mockVertxRegistry));
    }

    @Test
    void testNamingConventionAppliedForPrometheusRegistry() {
        StrimziCollectorRegistry mockStrimziRegistry = mock(StrimziCollectorRegistry.class);
        PrometheusMeterRegistry mockVertxRegistry = mock(PrometheusMeterRegistry.class);
        MeterRegistry.Config meterRegistryConfig = mock(MeterRegistry.Config.class);
        when(mockVertxRegistry.config()).thenReturn(meterRegistryConfig);
        
        new MetricsReporter(null, mockStrimziRegistry, mockVertxRegistry);

        verify(mockVertxRegistry).config();
    }
}
