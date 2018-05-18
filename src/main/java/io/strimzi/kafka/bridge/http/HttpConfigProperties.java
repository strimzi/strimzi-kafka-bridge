package io.strimzi.kafka.bridge.http;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * HTTP configuration properties
 */
@Component
@ConfigurationProperties(prefix = "http")
public class HttpConfigProperties {

    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final int DEFAULT_PORT = 8080;

    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

}
