/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.SinkBridgeEndpoint;
import io.strimzi.kafka.bridge.SourceBridgeEndpoint;
import io.vertx.core.http.HttpConnection;

import java.util.HashMap;
import java.util.Map;

/**
 * Context class which is used for storing endpoints.
 * Using context in lower-level classes for better state determination.
 */
public class HttpBridgeContext {
    private Map<String, SinkBridgeEndpoint> httpSinkEndpoints = new HashMap<>();
    private Map<HttpConnection, SourceBridgeEndpoint> httpSourceEndpoints = new HashMap<>();


    /**
     * @return map of sink endpoints
     */
    public Map<String, SinkBridgeEndpoint> getHttpSinkEndpoints() {
        return this.httpSinkEndpoints;
    }

    /**
     * @return map of source endpoints
     */
    public Map<HttpConnection, SourceBridgeEndpoint> getHttpSourceEndpoints() {
        return this.httpSourceEndpoints;
    }

}
