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

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;

/**
 * An exception that can be converted into an {@code ErrorCondition}.
 */
class AmqpErrorConditionException extends Exception {

    private static final long serialVersionUID = 887822732457738920L;

    private final String error;

    public AmqpErrorConditionException(String error, String message, Throwable cause) {
        super(message, cause);
        this.error = error;
    }

    public AmqpErrorConditionException(String error, String message) {
        super(message);
        this.error = error;
    }

    /**
     * Convert this exception into an {@code ErrorCondition}.
     */
    public ErrorCondition toCondition() {
        return new ErrorCondition(Symbol.getSymbol(this.error), getMessage());
    }

}
