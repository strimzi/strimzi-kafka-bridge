package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.config.BridgeConfigProperties;
import org.springframework.stereotype.Component;

/**
 * Bridge configuration properties for HTTP support
 */
@Component
public class HttpBridgeConfigProperties extends BridgeConfigProperties<HttpConfigProperties> {

    public HttpBridgeConfigProperties(){
        super();
        this.endpointConfigProperties = new HttpConfigProperties();
    }
}
