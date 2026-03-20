/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for HttpBridgeExecutor
 */
public class HttpBridgeExecutorTest {
    private static final Logger LOGGER = LogManager.getLogger(HttpBridgeExecutorTest.class);

    private ExecutorService executor;

    @AfterEach
    public void cleanup() {
        if (executor != null && !executor.isShutdown()) {
            HttpBridgeExecutor.shutdown(executor);
        }
    }

    @Test
    public void testExecutorCreation() {
        executor = HttpBridgeExecutor.create(4, 100);

        assertThat(executor instanceof ThreadPoolExecutor, is(true));

        ThreadPoolExecutor tpe = (ThreadPoolExecutor) executor;
        assertThat(tpe.getCorePoolSize(), is(4));
        assertThat(tpe.getMaximumPoolSize(), is(4));
        assertThat(tpe.getQueue().remainingCapacity(), is(100));
    }

    @Test
    public void testThreadNaming() throws Exception {
        executor = HttpBridgeExecutor.create(2, 10);

        List<String> threadNames = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(3);

        // submit 3 tasks to capture thread names
        for (int i = 0; i < 3; i++) {
            executor.submit(() -> {
                threadNames.add(Thread.currentThread().getName());
                latch.countDown();
            });
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Tasks should complete within timeout");

        // verify all threads have the expected name pattern
        for (String name : threadNames) {
            assertThat(name, startsWith("kafka-bridge-async-"));
        }
    }

    @Test
    public void testThreadsAreNonDaemon() throws Exception {
        executor = HttpBridgeExecutor.create(3, 10);

        AtomicInteger daemonCount = new AtomicInteger(0);
        AtomicInteger nonDaemonCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(3);

        // submit 3 tasks to verify multiple threads are non-daemon
        for (int i = 0; i < 3; i++) {
            executor.submit(() -> {
                LOGGER.info("Running thread {}, daemon = {}", Thread.currentThread().getName(), Thread.currentThread().isDaemon());
                if (Thread.currentThread().isDaemon()) {
                    daemonCount.incrementAndGet();
                } else {
                    nonDaemonCount.incrementAndGet();
                }
                latch.countDown();
            });
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Tasks should complete within timeout");
        assertThat("All threads should be non-daemon", daemonCount.get(), is(0));
        assertThat("Should have created non-daemon threads", nonDaemonCount.get(), is(3));
    }

    @Test
    public void testPoolSizeRespected() throws Exception {
        int poolSize = 2;
        executor = HttpBridgeExecutor.create(poolSize, 10);

        CountDownLatch startLatch = new CountDownLatch(poolSize);
        CountDownLatch finishLatch = new CountDownLatch(1);

        // submit tasks that will block until we release them
        for (int i = 0; i < poolSize; i++) {
            executor.submit(() -> {
                startLatch.countDown();
                LOGGER.info("Thread {} is blocking the pool", Thread.currentThread().getName());
                try {
                    finishLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        // wait for all pool threads to be active
        assertTrue(startLatch.await(5, TimeUnit.SECONDS), "All pool threads should start");

        ThreadPoolExecutor tpe = (ThreadPoolExecutor) executor;
        assertThat("Active threads should not exceed pool size", tpe.getActiveCount(), lessThanOrEqualTo(poolSize));

        finishLatch.countDown();
    }

    @Test
    public void testAbortPolicyOnPoolAndQueueFull() throws Exception {
        int poolSize = 1;
        int queueSize = 1;
        executor = HttpBridgeExecutor.create(poolSize, queueSize);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch blockLatch = new CountDownLatch(1);

        // fill the pool (1 thread), this task will block
        executor.submit(() -> {
            startLatch.countDown(); // signal that this thread has started
            LOGGER.info("Thread {} is blocking the pool", Thread.currentThread().getName());
            try {
                blockLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // wait for the pool thread to actually start and be blocking
        assertTrue(startLatch.await(5, TimeUnit.SECONDS), "Pool thread should start");

        // fill the queue (1 task), just needs to occupy a queue slot
        executor.submit(() -> { });

        // now: pool is full (1 thread blocked) + queue is full (1 task waiting)
        // this next task should be rejected
        assertThrows(RejectedExecutionException.class, () -> {
            executor.submit(() -> { });
        });

        // release all blocked tasks
        blockLatch.countDown();
    }

    @Test
    public void testQueueSizeRespected() throws Exception {
        int poolSize = 1;
        int queueSize = 5;
        executor = HttpBridgeExecutor.create(poolSize, queueSize);

        ThreadPoolExecutor tpe = (ThreadPoolExecutor) executor;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch blockLatch = new CountDownLatch(1);

        // fill the pool
        executor.submit(() -> {
            startLatch.countDown(); // signal that this thread has started
            LOGGER.info("Thread {} is blocking the pool", Thread.currentThread().getName());
            try {
                blockLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // wait for the pool thread to actually start and be blocking
        assertTrue(startLatch.await(5, TimeUnit.SECONDS), "Pool thread should start");

        // fill the queue
        for (int i = 0; i < queueSize; i++) {
            executor.submit(() -> { });
        }

        assertThat("Queue should have tasks", tpe.getQueue().size(), greaterThan(0));
        assertThat("Queue size should not exceed configured size", tpe.getQueue().size(), lessThanOrEqualTo(queueSize));

        // release all blocked tasks
        blockLatch.countDown();
    }

    @Test
    public void testGracefulShutdown() throws Exception {
        executor = HttpBridgeExecutor.create(2, 10);

        AtomicInteger completedTasks = new AtomicInteger(0);

        // submit some quick tasks
        for (int i = 0; i < 5; i++) {
            executor.submit(() -> {
                completedTasks.incrementAndGet();
            });
        }

        // shutdown and verify it completes
        HttpBridgeExecutor.shutdown(executor);

        assertTrue(executor.isShutdown(), "Executor should be shutdown");
        assertTrue(executor.isTerminated(), "Executor should be terminated");
        assertThat("All tasks should complete", completedTasks.get(), is(5));
    }

    @Test
    public void testShutdownWithNull() {
        // should not throw exception
        HttpBridgeExecutor.shutdown(null);
    }

    @Test
    public void testConcurrentTaskExecution() throws Exception {
        int poolSize = 4;
        executor = HttpBridgeExecutor.create(poolSize, 100);

        int taskCount = 20;
        AtomicInteger completedTasks = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(taskCount);

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < taskCount; i++) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                completedTasks.incrementAndGet();
                latch.countDown();
            }, executor);
            futures.add(future);
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS), "All tasks should complete");
        assertThat(completedTasks.get(), is(taskCount));

        // wait for all futures to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS);
    }
}
