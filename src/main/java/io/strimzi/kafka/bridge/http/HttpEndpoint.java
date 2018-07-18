/*
 * Copyright 2018 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.Endpoint;
import io.vertx.core.http.HttpServerRequest;

public class HttpEndpoint implements Endpoint<HttpServerRequest> {

    private HttpServerRequest httpServerRequest;

    public HttpEndpoint(HttpServerRequest httpServerRequest){
        this.httpServerRequest = httpServerRequest;
    }
    @Override
    public HttpServerRequest get() {
        return httpServerRequest;
    }
}