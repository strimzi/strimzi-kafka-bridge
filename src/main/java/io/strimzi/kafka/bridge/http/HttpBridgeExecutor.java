/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import io.vertx.micrometer.backends.BackendRegistries;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory for creating the dedicated ExecutorService for Kafka-related asynchronous operations.
 */
public class HttpBridgeExecutor {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeExecutor.class);

    /**
     * Custom ThreadFactory for Kafka-related asynchronous operations.
     * Creates named, non-daemon threads with uncaught exception handling.
     */
    private static class HttpBridgeThreadFactory implements ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("kafka-bridge-executor-" + counter.incrementAndGet());
            t.setDaemon(false); // Non-daemon threads for graceful shutdown
            t.setUncaughtExceptionHandler((thread, ex) ->
                LOGGER.error("Uncaught exception in {}", thread.getName(), ex));
            return t;
        }
    }

    /**
     * Create a dedicated ExecutorService for Kafka-related asynchronous operations.
     *
     * @param poolSize the number of threads in the pool
     * @param queueSize the maximum number of queued tasks
     * @return configured ExecutorService
     */
    public static ExecutorService create(int poolSize, int queueSize) {
        LOGGER.info("Creating bridge executor with poolSize={}, queueSize={}", poolSize, queueSize);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            poolSize,
            poolSize,
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(queueSize),
            new HttpBridgeThreadFactory(),
            new ThreadPoolExecutor.AbortPolicy() // if both pool and queue are full, a task is rejected (throws RejectedExecutionException)
        );

        // allow core threads to timeout during idle periods
        // if no tasks come within the keep alive timeout, threads in the pool are no longer active
        executor.allowCoreThreadTimeOut(true);

        // register metrics with Micrometer if available
        registerMetrics(executor);

        return executor;
    }

    /**
     * Register executor metrics with Micrometer.
     *
     * @param executor the executor to monitor
     */
    private static void registerMetrics(ThreadPoolExecutor executor) {
        try {
            MeterRegistry registry = BackendRegistries.getDefaultNow();
            if (registry != null) {
                // Micrometer's built-in ExecutorService metrics (automatic)
                ExecutorServiceMetrics.monitor(
                    registry,
                    executor,
                    "kafka-bridge"  // executor name (appears as 'name' tag in metrics)
                );
                LOGGER.info("Bridge executor metrics registered with Micrometer");
            } else {
                LOGGER.debug("MeterRegistry not available, bridge executor metrics will not be exposed");
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to register executor metrics", e);
        }
    }

    /**
     * Shutdown the ExecutorService gracefully.
     *
     * @param executor the executor to shutdown
     */
    public static void shutdown(ExecutorService executor) {
        if (executor == null) {
            return;
        }

        LOGGER.info("Shutting down bridge executor");
        executor.shutdown();

        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOGGER.warn("Bridge executor did not terminate gracefully, forcing shutdown");
                executor.shutdownNow();

                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    LOGGER.error("Bridge executor did not terminate after forced shutdown");
                }
            }
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while waiting for bridge executor shutdown", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
