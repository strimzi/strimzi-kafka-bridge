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
 * Represents an error related to HTTP bridging
 */
public class HttpBridgeError {

    private final int code;
    private final String message;

    /**
     * Constructor
     *
     * @param code code classifying the error itself
     * @param message message providing more information about the error
     */
    public HttpBridgeError(int code, String message) {
        this.code = code;
        this.message = message;
    }

    /**
     * @return code classifying the error itself
     */
    public int getCode() {
        return code;
    }

    /**
     * @return message providing more information about the error
     */
    public String getMessage() {
        return message;
    }
}
