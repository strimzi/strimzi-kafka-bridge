/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.tracing;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextStorage;
import io.opentelemetry.context.ContextStorageProvider;
import io.opentelemetry.context.Scope;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provider returning the custom OpenTelemetry context storage
 */
public class BridgeContextStorageProvider implements ContextStorageProvider {
    static final String ACTIVE_CONTEXT = "tracing.context";

    /**
     * Constructor
     */
    public BridgeContextStorageProvider() {
    }

    @Override
    public ContextStorage get() {
        return BridgeContextStorage.INSTANCE;
    }

    /**
     * Custom OpenTelemetry context storage
     * The Vert.x context storage cannot be used anymore having a part of the bridge not using a Vert.x instance (so NPEs around the corner)
     * This custom implementation provides the same behaviour as the Vert.x one but without the need of a Vert.x instance
     * It's needed in a transitioning phase with parts of the bridge still handled by Vert.x where the event loop is around,
     * see for example incoming HTTP requests
     * In this case the default OpenTelemetry ThreadLocalContextStorage doesn't work because for each new incoming HTTP request
     * the Context is the same as the previous request because bounded to the thread (Vert.x event loop) while it's should be new
     * It drives to spans grouped all together under the same trace when they are related to different HTTP requests
     * We should be able to get rid of this class when Vert.x will be totally out of the picture
     */
    // TODO: evaluate to remove this class, back to the default ThreadLocalContextStorage, when Vert.x will be totally removed
    enum BridgeContextStorage implements ContextStorage {
        INSTANCE;

        private final ConcurrentMap<Object, Object> data = new ConcurrentHashMap();

        @Override
        public Scope attach(Context toAttach) {
            Context current = (Context) data.get(ACTIVE_CONTEXT);
            if (current == toAttach) {
                return Scope.noop();
            } else {
                data.put(ACTIVE_CONTEXT, toAttach);
                return current == null ? () -> {
                    data.remove(ACTIVE_CONTEXT);
                } : () -> {
                    data.put(ACTIVE_CONTEXT, current);
                };
            }
        }

        @Override
        public Context current() {
            return (Context) data.get(ACTIVE_CONTEXT);
        }
    }
}
