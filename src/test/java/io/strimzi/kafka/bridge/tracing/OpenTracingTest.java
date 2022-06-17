/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.vertx.core.tracing.TracingOptions;
import io.vertx.tracing.opentracing.OpenTracingOptions;

/**
 * OpenTracing tests
 *
 * These env vars need to be set:
 * * JAEGER_SERVICE_NAME=ot_kafka_bridge_test
 * * JAEGER_SAMPLER_TYPE=const
 * * JAEGER_SAMPLER_PARAM=1
 */
public class OpenTracingTest extends TracingTestBase {
    @Override
    protected TracingOptions tracingOptions() {
        return new OpenTracingOptions();
    }
}
