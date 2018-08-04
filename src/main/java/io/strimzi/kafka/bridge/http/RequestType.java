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
 * This enum contains types of requests which maps to operations on kafka
 */
public enum RequestType {

    //produce records
    PRODUCE,

    //consumer creation
    CREATE,

    //subscribe to topic
    SUBSCRIBE,

    //consume records
    CONSUME,

    //commit offsets
    OFFSETS,

    //invalid request
    INVALID,

    //Delete consumer
    DELETE
}