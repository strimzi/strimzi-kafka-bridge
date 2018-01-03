/*
 * Copyright 2016 Red Hat Inc.
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

package io.strimzi.kafka.bridge.amqp;

/**
 * The AMQP side working mode (client or server)
 */
public enum AmqpMode {

    CLIENT("client"),
    SERVER("server");

    private final String mode;

    private AmqpMode(String mode) {
        this.mode = mode;
    }

    /**
     * Get the enum value from the corresponding string value
     *
     * @param mode  mode as a string
     * @return  mode as enum value
     */
    public static AmqpMode from(String mode) {
        if (mode.equals(CLIENT.mode)) {
            return CLIENT;
        } else if (mode.equals(SERVER.mode)) {
            return SERVER;
        } else {
            throw new IllegalArgumentException("Unknown mode: " + mode);
        }
    }
}
