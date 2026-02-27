/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.extensions;

import io.strimzi.kafka.bridge.objects.TestStorage;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * Extension handling preparation, clean-up and injection of {@link TestStorage} into the tests.
 */
public class TestStorageExtension implements BeforeEachCallback, AfterEachCallback, ParameterResolver {

    private static final String TEST_STORAGE_KEY = "testStorage";

    /**
     * Method creating {@link TestStorage} for the particular test and {@link ExtensionContext}.
     *
     * @param extensionContext  context of the test.
     */
    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext);
        getStore(extensionContext).put(TEST_STORAGE_KEY, testStorage);
    }

    /**
     * Method that cleans up services after the tests.
     *
     * @param extensionContext  context of the test.
     */
    @Override
    public void afterEach(ExtensionContext extensionContext) {
        TestStorage testStorage = getStore(extensionContext).get(TEST_STORAGE_KEY, TestStorage.class);
        if (testStorage != null && testStorage.getAdminClientFacade() != null) {
            testStorage.getAdminClientFacade().close();
        }
    }

    /**
     * Method that checks if the parameter is supported by the class.
     *
     * @param parameterContext  context of the parameter.
     * @param extensionContext  context of the test.
     *
     * @return  boolean value representing if the parameter is supported by this class.
     */
    @Override
    public boolean supportsParameter(ParameterContext parameterContext,
                                     ExtensionContext extensionContext) {
        Class<?> type = parameterContext.getParameter().getType();
        return type.equals(TestStorage.class);
    }

    /**
     * Method used for injecting the parameter into the tests.
     *
     * @param parameterContext  context of the parameter.
     * @param extensionContext  context of the test.
     *
     * @return  parameter from the {@link ExtensionContext} store.
     */
    @Override
    public Object resolveParameter(ParameterContext parameterContext,
                                   ExtensionContext extensionContext) {
        return getStore(extensionContext).get(TEST_STORAGE_KEY, TestStorage.class);
    }

    /**
     * Method for getting the store of the extension context.
     *
     * @param extensionContext  context of the test.
     *
     * @return  store of the extension context.
     */
    private ExtensionContext.Store getStore(ExtensionContext extensionContext) {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL);
    }
}