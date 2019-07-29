package io.strimzi.kafka.bridge.http.service;

import io.vertx.ext.web.client.WebClient;


public class BaseService <B extends BaseService> {

    WebClient webClient;

    static final String BRIDGE_HOST = "127.0.0.1";
    static final int BRIDGE_PORT = 8080;


    public BaseService(WebClient webClient) {
        this.webClient = webClient;
    }


}
