/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.objects;

import io.strimzi.kafka.bridge.clients.BasicKafkaClient;
import io.strimzi.kafka.bridge.extensions.BridgeExtension;
import io.strimzi.kafka.bridge.extensions.KafkaExtension;
import io.strimzi.kafka.bridge.facades.AdminClientFacade;
import io.strimzi.kafka.bridge.httpclient.HttpService;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Random;

/**
 * Class used for storing variables, services, and all information used in tests.
 */
public class BridgeTestContext {
    private final static Random RNG = new Random();

    private String topicName;
    private HttpService httpService;
    private HttpService managementHttpService;
    private AdminClientFacade adminClientFacade;
    private BasicKafkaClient basicKafkaClient;
    private String bridgeHost;
    private int bridgePort;

    /**
     * Constructor for creating variables and services for particular {@param extensionContext}.
     *
     * @param extensionContext  context of the test.
     */
    public BridgeTestContext(ExtensionContext extensionContext) {
        topicName = "my-topic-" + RNG.nextInt(Integer.MAX_VALUE);
        httpService = BridgeExtension.getHttpService(extensionContext);
        managementHttpService = BridgeExtension.getManagementHttpService(extensionContext);
        adminClientFacade = AdminClientFacade.create(KafkaExtension.getKafkaCluster(extensionContext).getBootstrapServers());
        basicKafkaClient = new BasicKafkaClient(KafkaExtension.getKafkaCluster(extensionContext).getBootstrapServers());
        bridgeHost = BridgeExtension.getBridgeHost(extensionContext);
        bridgePort = BridgeExtension.getBridgePort(extensionContext);
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
     * Returns management {@link HttpService}.
     * @return management {@link HttpService}.
     */
    public HttpService getManagementHttpService() {
        return managementHttpService;
    }

    /**
     * Returns {@link AdminClientFacade}.
     * @return {@link AdminClientFacade}.
     */
    public AdminClientFacade getAdminClientFacade() {
        return adminClientFacade;
    }

    public BasicKafkaClient getBasicKafkaClient() {
        return basicKafkaClient;
    }

    /**
     * Returns the bridge host.
     * @return the bridge host.
     */
    public String getBridgeHost() {
        return bridgeHost;
    }

    /**
     * Returns the bridge main port (HTTP or HTTPS depending on SSL configuration).
     * @return the bridge main port.
     */
    public int getBridgePort() {
        return bridgePort;
    }

}
