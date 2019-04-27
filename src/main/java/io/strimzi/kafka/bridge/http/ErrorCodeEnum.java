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

package io.strimzi.kafka.bridge.http;

import java.util.HashMap;
import java.util.Map;

public enum ErrorCodeEnum {
    TOPIC_NOT_FOUND(40401),
    PARTITION_NOT_FOUND(40402),
    CONSUMER_NOT_FOUND(40403),
    CONSUMER_ALREADY_EXISTS(40902),
    BAD_REQUEST(400),
    NOT_FOUND(404),
    UNPROCESSABLE_ENTITY(422),
    INTERNAL_SERVER_ERROR(500),
    NO_CONTENT(204);

    private int value;
    private static Map map = new HashMap<>();

    ErrorCodeEnum(int value) {
        this.value = value;
    }

    static {
        for (ErrorCodeEnum pageType : ErrorCodeEnum.values()) {
            map.put(pageType.value, pageType);
        }
    }

    public static ErrorCodeEnum valueOf(int pageType) {
        return (ErrorCodeEnum) map.get(pageType);
    }

    public int getValue() {
        return value;
    }
}
