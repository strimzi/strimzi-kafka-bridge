/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.objects;

import io.strimzi.kafka.bridge.extensions.BridgeExtension;
import io.strimzi.kafka.bridge.extensions.KafkaExtension;
import io.strimzi.kafka.bridge.facades.AdminClientFacade;
import io.strimzi.kafka.bridge.httpclient.HttpService;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Random;

/**
 * Class used for storing variables, services, and all information used in tests.
 */
public class TestStorage {
    private final static Random RNG = new Random();

    private String topicName;
    private HttpService httpService;
    private AdminClientFacade adminClientFacade;

    /**
     * Constructor for creating variables and services for particular {@param extensionContext}.
     *
     * @param extensionContext  context of the test.
     */
    public TestStorage(ExtensionContext extensionContext) {
        topicName = "my-topic-" + RNG.nextInt(Integer.MAX_VALUE);
        httpService = BridgeExtension.getHttpService(extensionContext);
        adminClientFacade = AdminClientFacade.create(KafkaExtension.getKafkaCluster(extensionContext).getBootstrapServers());
    }

    /**
     * Returns topic name.
     * @return topic name.
     */
    public String getTopicName() {
        return topicName;
    }

    /**
     * Returns {@link HttpService}.
     * @return {@link HttpService}.
     */
    public HttpService getHttpService() {
        return httpService;
    }

    /**
     * Returns {@link AdminClientFacade}.
     * @return {@link AdminClientFacade}.
     */
    public AdminClientFacade getAdminClientFacade() {
        return adminClientFacade;
    }
}
