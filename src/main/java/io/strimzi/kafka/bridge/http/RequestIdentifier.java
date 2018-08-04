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

import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;

/**
 * This class contains utility functions which identifies the RequestType based on several factors
 */
public class RequestIdentifier {

    static RequestType getRequestType(HttpServerRequest request){

        final HttpMethod method =  request.method();

        final String requestPath =  request.path();

        //request path always starts with "/" which is not required in path parameters
        String [] params = requestPath.substring(1).split("/");

        switch (params[0]){

            case "topic":
                //produce records
                //request type = POST
                //path = topic/{topic-name}
                //spliting this path will return an array of length 2
                //param[0] = "topic", param[1] = {topic-name}
                if (method == HttpMethod.POST && params.length == 2){
                    return RequestType.PRODUCE;
                }
                break;

            //all consumer enpoints starts with "/consumers"
            case "consumers":

                switch (method){
                    case POST:
                        //consumer creation
                        //request type = POST
                        //path = consumers/{consumer-group}
                        //spliting this path will return an array of length 2.
                        //param[0] = "consumers", param[1] = {consumer-group}
                        if (params.length == 2) {
                            return RequestType.CREATE;
                        }

                        //consumer subscription
                        //request type = POST
                        //path = consumers/{consumer-group}/instances/{instance-id)/subscription
                        //spliting this path will return an array of length 5
                        //param[0] = "consumers", param[1] = {consumer-group}, param[2] = "instances", param[3] = {instance-id}, param[4] = "subscription"
                        else  if (params.length == 5 && params[params.length - 1].equals("subscription")) {
                            return RequestType.SUBSCRIBE;
                        }

                        //commit offsets
                        //request type = POST
                        //path = consumers/{consumer-group}/instances/{instance-id)/offsets
                        //spliting this path will return an array of length 5
                        //param[0] = "consumers", param[1] = {consumer-group}, param[2] = "instances", param[3] = {instance-id}, param[4] = "offsets"
                        else if (params.length == 5 && params[params.length - 1].equals("offsets")){
                            return RequestType.OFFSETS;
                        }

                        break;

                    case GET:

                        //consume records
                        //request type = GET
                        //path = consumers/{consumer-group}/instances/{instance-id)/records
                        //spliting this path will return an array of length 5
                        //param[0] = "consumers", param[1] = {consumer-group}, param[2] = "instances", param[3] = {instance-id}, param[4] = "records"
                        if (params.length == 5 && params[params.length - 1].equals("records")) {
                            return RequestType.CONSUME;
                        }

                        break;

                    case DELETE:

                        //delete consumer instance
                        //request type = DELETE
                        //path = consumers/{consumer-group}/instances/{instance-id)
                        //spliting this path will return an array of length 4
                        //param[0] = "consumers", param[1] = {consumer-group}, param[2] = "instances", param[3] = {instance-id}
                        if (params.length == 4) {
                            return RequestType.DELETE;
                        }

                        break;
                }

                break;

        }

        return RequestType.INVALID;
    }

}