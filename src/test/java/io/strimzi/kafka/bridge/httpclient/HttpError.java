/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.httpclient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public record HttpError(int code, String message, List<String> validationErrors) {

    public static HttpError fromResponse(String responseBody) {
        Map<String, Object> errorBody = HttpResponseUtils.getResponseAsMap(responseBody);
        List<String> validationErrors = new ArrayList<>();

        if (errorBody.containsKey("validation_errors")) {
            List.of(((Object[]) errorBody.get("validation_errors"))).forEach(error -> validationErrors.add((String) error));
        }

        return new HttpError(Integer.valueOf(errorBody.get("error_code").toString()), (String) errorBody.get("message"), validationErrors);
    }
}
