/*
 * Copyright 2019 Red Hat Inc.
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

package io.strimzi.kafka.bridge.http.model;

/**
 * This class represents a result of an HTTP bridging operation
 *
 * @param <T> the class bringing the actual result as {@link HttpBridgeError} or {@link io.vertx.kafka.client.producer.RecordMetadata}
 */
public class HttpBridgeResult<T> {

    final T result;

    /**
     * Constructor
     *
     * @param result actual result
     */
    public HttpBridgeResult(T result) {
        this.result = result;
    }

    /**
     * @return the actual result
     */
    public T getResult() {
        return result;
    }
}
