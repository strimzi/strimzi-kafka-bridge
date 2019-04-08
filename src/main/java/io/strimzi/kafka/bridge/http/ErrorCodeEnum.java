package io.strimzi.kafka.bridge.http;

import java.util.HashMap;
import java.util.Map;

public enum ErrorCodeEnum {
    TOPIC_NOT_FOUND(40401),
    PARTITION_NOT_FOUND(40402),
    INVALID_REQUEST(400),
    UNKNOWN_REQUEST(404),
    EMPTY_REQUEST(422),
    UNKNOWN_ERROR(500);

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
