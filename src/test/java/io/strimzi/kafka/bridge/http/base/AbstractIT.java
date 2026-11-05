/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.base;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.objects.BridgeTestContext;
import org.junit.jupiter.params.Parameter;

import java.net.http.HttpResponse;
import java.util.Random;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public abstract class AbstractIT {
    protected static final int TEST_TIMEOUT = 60;
    protected static final ObjectMapper MAPPER = new ObjectMapper();

    /** name of the rejected authentications counter (Prometheus format) exposed on the /metrics endpoint */
    private static final String REJECTED_AUTH_METRIC = "strimzi_bridge_auth_rejected_total";

    /**
     * The {@link Parameter} kafkaVersion here is needed because of the
     * {@link org.junit.jupiter.params.ParameterizedClass}. Without that, we would not be
     * able to run all the tests against multiple versions of Kafka (and the tests would throw errors).
     */
    @Parameter
    String kafkaVersion;

    protected String generateRandomConsumerName() {
        int salt = new Random().nextInt(Integer.MAX_VALUE);
        return "my-kafka-consumer-" + salt;
    }

    protected String generateRandomConsumerGroupName() {
        int salt = new Random().nextInt(Integer.MAX_VALUE);
        return "my-group-" + salt;
    }

    /**
     * Assert that the rejected authentications counter with the provided reason is exposed
     * on the /metrics endpoint (Prometheus text format) with a positive value
     *
     * @param bridgeTestContext the bridge test context
     * @param reason            the rejection reason
     */
    protected void assertRejectedAuthMetric(BridgeTestContext bridgeTestContext, String reason) {
        HttpResponse<String> response = bridgeTestContext.getManagementHttpService().get("/metrics");
        assertThat(response.statusCode(), is(HttpResponseStatus.OK.code()));
        boolean found = false;
        for (String line : response.body().split("\n")) {
            if (line.startsWith(REJECTED_AUTH_METRIC) && line.contains("reason=\"" + reason + "\"")) {
                found = true;
                double value = Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
                assertThat("rejected authentications counter value with reason " + reason,
                    value, greaterThanOrEqualTo(1.0));
                break;
            }
        }
        assertThat("metric " + REJECTED_AUTH_METRIC + "{reason=\"" + reason + "\"} not found", found, is(true));
    }
}
