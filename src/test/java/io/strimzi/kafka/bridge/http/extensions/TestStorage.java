/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.extensions;

import io.strimzi.kafka.bridge.facades.AdminClientFacade;
import io.strimzi.kafka.bridge.httpclient.HttpRequestHandler;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Random;

public class TestStorage {
    private final static Random RNG = new Random();

    private String topicName;
    private HttpRequestHandler httpRequestHandler;
    private AdminClientFacade adminClientFacade;

    public TestStorage(ExtensionContext extensionContext) {
        topicName = "my-topic-" + RNG.nextInt(Integer.MAX_VALUE);
        httpRequestHandler = BridgeExtension.getHttpRequestHandler(extensionContext);
        adminClientFacade = AdminClientFacade.create(KafkaExtension.getKafkaCluster(extensionContext).getBootstrapServers());
    }

    public String getTopicName() {
        return topicName;
    }

    public HttpRequestHandler getHttpRequestHandler() {
        return httpRequestHandler;
    }

    public AdminClientFacade getAdminClientFacade() {
        return adminClientFacade;
    }
}
