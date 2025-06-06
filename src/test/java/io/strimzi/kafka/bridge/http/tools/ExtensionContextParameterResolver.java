/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.tools;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;


/**
 * JUnit 5 {@link ParameterResolver} that injects the current {@link ExtensionContext}
 * into test methods or lifecycle methods that declare it as a parameter.
 * <p>
 * This is typically used to provide test context to {@code @BeforeEach}, {@code @AfterEach},
 * or test methods themselves.
 * </p>
 */
public class ExtensionContextParameterResolver implements ParameterResolver {
    /**
     * Checks if the parameter is of type {@link ExtensionContext}.
     *
     * @param parameterContext The context for the parameter for which a value is to be resolved.
     * @param extensionContext The extension context for the test or container.
     * @return {@code true} if the parameter is of type {@code ExtensionContext}, otherwise {@code false}.
     * @throws ParameterResolutionException If an error occurs while checking parameter support.
     */
    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType() == ExtensionContext.class;
    }

    /**
     * Provides the {@link ExtensionContext} instance as the parameter value.
     *
     * @param parameterContext The context for the parameter for which a value is to be resolved.
     * @param extensionContext The extension context for the test or container.
     * @return The current {@code ExtensionContext} instance.
     * @throws ParameterResolutionException If an error occurs during parameter resolution.
     */
    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return extensionContext;
    }
}
