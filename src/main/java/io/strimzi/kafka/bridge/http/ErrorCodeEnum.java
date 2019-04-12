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
    INTERNAL_SERVER_ERROR(500);

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
