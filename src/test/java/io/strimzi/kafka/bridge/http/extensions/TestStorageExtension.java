/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.bridge.http.extensions;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

public class TestStorageExtension implements BeforeEachCallback, AfterEachCallback, ParameterResolver {

    private static final String TEST_STORAGE_KEY = "testStorage";

    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext);
        getStore(extensionContext).put(TEST_STORAGE_KEY, testStorage);
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        TestStorage testStorage = getStore(extensionContext).get(TEST_STORAGE_KEY, TestStorage.class);
        if (testStorage != null && testStorage.getAdminClientFacade() != null) {
            testStorage.getAdminClientFacade().close();
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext,
                                     ExtensionContext extensionContext) {
        Class<?> type = parameterContext.getParameter().getType();
        return type.equals(TestStorage.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext,
                                   ExtensionContext extensionContext) {
        return getStore(extensionContext).get(TEST_STORAGE_KEY, TestStorage.class);
    }

    private ExtensionContext.Store getStore(ExtensionContext context) {
        return context.getStore(ExtensionContext.Namespace.GLOBAL);
    }
}