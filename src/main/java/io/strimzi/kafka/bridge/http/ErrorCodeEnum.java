/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
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
    NOT_ACCEPTABLE(406),
    UNPROCESSABLE_ENTITY(422),
    INTERNAL_SERVER_ERROR(500),
    NO_CONTENT(204),
    CONFLICT(409);

    private int value;
    private static Map<Integer, ErrorCodeEnum> map = new HashMap<>();

    ErrorCodeEnum(int value) {
        this.value = value;
    }

    static {
        for (ErrorCodeEnum errorCodeEnum : ErrorCodeEnum.values()) {
            map.put(errorCodeEnum.value, errorCodeEnum);
        }
    }

    public static ErrorCodeEnum valueOf(int errorCodeEnum) {
        return (ErrorCodeEnum) map.get(errorCodeEnum);
    }

    public int getValue() {
        return value;
    }
}
