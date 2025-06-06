/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.tools;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;

/**
 * Provides a separator in the log output before and after each test for improved readability.
 * <p>
 * Implement this interface in your test classes to automatically log a separator line and test status
 * (STARTED, SUCCEEDED, FAILED) around each test execution.
 * </p>
 */
@ExtendWith(ExtensionContextParameterResolver.class)
public interface TestSeparator {
    /**
     * Logger instance used for logging separator and test status.
     */
    Logger LOGGER = LogManager.getLogger(TestSeparator.class);

    /**
     * The character used to build the separator line in the log output.
     */
    String SEPARATOR_CHAR = "#";

    /**
     * The length of the separator line.
     */
    int SEPARATOR_LENGTH = 76;

    /**
     * Logs a separator line and the test class/method at the start of each test.
     *
     * @param context the JUnit extension context for the test
     */
    @BeforeEach
    default void beforeEachTest(ExtensionContext context) {
        LOGGER.info(String.join("", Collections.nCopies(SEPARATOR_LENGTH, SEPARATOR_CHAR)));
        LOGGER.info("{}.{} - STARTED", context.getRequiredTestClass().getName(), context.getRequiredTestMethod().getName());
    }

    /**
     * Logs the test result (SUCCEEDED or FAILED) and a separator line at the end of each test.
     *
     * @param context the JUnit extension context for the test
     */
    @AfterEach
    default void afterEachTest(ExtensionContext context) {
        String status = context.getExecutionException().isPresent() ? "FAILED" : "SUCCEEDED";
        LOGGER.info("{}.{} - {}", context.getRequiredTestClass().getName(), context.getRequiredTestMethod().getName(), status);
        LOGGER.info(String.join("", Collections.nCopies(SEPARATOR_LENGTH, SEPARATOR_CHAR)));
    }
}
