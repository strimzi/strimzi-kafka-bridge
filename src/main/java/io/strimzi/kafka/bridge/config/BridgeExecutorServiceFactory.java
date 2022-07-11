/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.config;

import io.strimzi.kafka.bridge.tracing.TracingUtil;
import io.vertx.core.spi.ExecutorServiceFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Custom Vertx ExecutorServiceFactory - delegate to tracing impl to provide one.
 * Initially it could be NoopTracingHandle that provides it - before Application is fully initialized,
 * then it should be actual tracing implementation - if there is one.
 * Shutdown should be done OK, since tracing delegate will also delegate shutdown to original,
 * or original itself will be used.
 */
public class BridgeExecutorServiceFactory implements ExecutorServiceFactory {
    @Override
    public ExecutorService createExecutor(ThreadFactory threadFactory, Integer concurrency, Integer maxConcurrency) {
        ExecutorService original = ExecutorServiceFactory.INSTANCE.createExecutor(threadFactory, concurrency, maxConcurrency);
        return new ExecutorService() {
            private ExecutorService delegate() {
                return TracingUtil.getTracing().adapt(original);
            }

            @Override
            public void shutdown() {
                delegate().shutdown();
            }

            @Override
            public List<Runnable> shutdownNow() {
                return delegate().shutdownNow();
            }

            @Override
            public boolean isShutdown() {
                return delegate().isShutdown();
            }

            @Override
            public boolean isTerminated() {
                return delegate().isTerminated();
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                return delegate().awaitTermination(timeout, unit);
            }

            @Override
            public <T> Future<T> submit(Callable<T> task) {
                return delegate().submit(task);
            }

            @Override
            public <T> Future<T> submit(Runnable task, T result) {
                return delegate().submit(task, result);
            }

            @Override
            public Future<?> submit(Runnable task) {
                return delegate().submit(task);
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
                return delegate().invokeAll(tasks);
            }

            @Override
            public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
                return delegate().invokeAll(tasks, timeout, unit);
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
                return delegate().invokeAny(tasks);
            }

            @Override
            public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                return delegate().invokeAny(tasks, timeout, unit);
            }

            @Override
            public void execute(Runnable command) {
                delegate().execute(command);
            }
        };
    }
}
