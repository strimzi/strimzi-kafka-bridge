/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.kafka.bridge.http.APImodels;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.strimzi.kafka.bridge.http.generator.Required;

public class Record {

    @Required
    @JsonProperty(value = "value", required = true)
    public Object value;

    @JsonProperty("key")
    public Object key;

    @JsonProperty("partition")
    public Integer partition;

    @JsonProperty("topic")
    public String topic;

    @JsonProperty("offset")
    public Integer offset;
}
