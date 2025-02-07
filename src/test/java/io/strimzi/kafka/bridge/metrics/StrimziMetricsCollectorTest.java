/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.metrics;

import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StrimziMetricsCollectorTest {
    @Test
    void shouldReturnMetrics() throws IOException {
        PrometheusRegistry mockPromRegistry = mock(PrometheusRegistry.class);
        MetricSnapshots mockSnapshots = mock(MetricSnapshots.class);
        when(mockPromRegistry.scrape()).thenReturn(mockSnapshots);
        
        PrometheusTextFormatWriter mockPromFormatter = mock(PrometheusTextFormatWriter.class);
        doAnswer(invocation -> {
            ByteArrayOutputStream stream = invocation.getArgument(0);
            stream.write("test_metric\n".getBytes(StandardCharsets.UTF_8));
            return null;
        }).when(mockPromFormatter).write(any(), any());

        MetricsCollector metricsCollector = new StrimziMetricsCollector(mockPromRegistry, mockPromFormatter);
        
        String result = metricsCollector.scrape();
        assertThat(result, containsString("test_metric"));
        assertThat(result.getBytes(StandardCharsets.UTF_8).length, is(result.length()));
    }

    @Test
    void shouldHandleIoException() throws IOException {
        PrometheusRegistry mockPromRegistry = mock(PrometheusRegistry.class);
        MetricSnapshots mockSnapshots = mock(MetricSnapshots.class);
        when(mockPromRegistry.scrape()).thenReturn(mockSnapshots);

        PrometheusTextFormatWriter mockPromFormatter = mock(PrometheusTextFormatWriter.class);
        doThrow(new IOException("Test exception"))
            .when(mockPromFormatter).write(any(ByteArrayOutputStream.class), Mockito.eq(mockSnapshots));

        MetricsCollector metricsCollector = new StrimziMetricsCollector(mockPromRegistry, mockPromFormatter);
        
        RuntimeException exception = assertThrows(RuntimeException.class, () -> metricsCollector.doScrape());
        assertThat(exception.getMessage(), containsString("Test exception"));
    }
}
