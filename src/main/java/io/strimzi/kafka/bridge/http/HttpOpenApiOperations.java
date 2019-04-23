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

/**
 * OpenAPI operations ids
 */
public enum HttpOpenApiOperations {

    SEND("send"),
    SEND_TO_PARTITION("sendToPartition"),
    CREATE_CONSUMER("createConsumer"),
    DELETE_CONSUMER("deleteConsumer"),
    SUBSCRIBE("subscribe"),
    UNSUBSCRIBE("unsubscribe"),
    POLL("poll"),
    COMMIT("commit"),
    SEEK("seek"),
    SEEK_TO_BEGINNING("seekToBeginning"),
    SEEK_TO_END("seekToEnd");

    private final String text;

    HttpOpenApiOperations(final String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }

}
