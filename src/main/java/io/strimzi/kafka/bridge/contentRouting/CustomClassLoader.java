/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html). 
 */

/**
 * Many thanks to Isuru Weerarathna for this terrific article:
 * https://medium.com/@isuru89/java-a-child-first-class-loader-cbd9c3d0305
 * Most of the code in this class is taken from his code.
 */

package io.strimzi.kafka.bridge.contentRouting;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class CustomClassLoader extends URLClassLoader {
    private final ClassLoader sysClzLoader;

    public CustomClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
        sysClzLoader = getSystemClassLoader();
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        
        Class<?> loadedClass = findLoadedClass(name);
        if (loadedClass == null) {
            try {
                loadedClass = findClass(name);
            } catch (ClassNotFoundException e) {
                loadedClass = super.loadClass(name, resolve);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (resolve) {
            resolveClass(loadedClass);
        }
        return loadedClass;
    }



    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        List<URL> allRes = new LinkedList<URL>();

        Enumeration<URL> sysResources = sysClzLoader.getResources(name);
        if (sysResources != null) {
            while (sysResources.hasMoreElements()) {
                allRes.add(sysResources.nextElement());
            }
        }

        Enumeration<URL> thisRes = findResources(name);
        if (thisRes != null) {
            while (thisRes.hasMoreElements()) {
                allRes.add(thisRes.nextElement());
            }
        }
        final List<URL> finalList = new LinkedList(allRes);

        Enumeration<URL> parentRes = super.findResources(name);
        if (parentRes != null) {
            while (parentRes.hasMoreElements()) {
                allRes.add(parentRes.nextElement());
            }
        }

        return new Enumeration<URL>() {
            Iterator<URL> it = finalList.iterator();

            @Override
            public boolean hasMoreElements() {
                return it.hasNext();
            }

            @Override
            public URL nextElement() {
                return it.next();
            }
        };
    }

    @Override
    public URL getResource(String name) {
        URL res = null;
        if (sysClzLoader != null) {
            res = sysClzLoader.getResource(name);
        }
        if (res == null) {
            res = findResource(name);
        }
        if (res == null) {
            res = super.getResource(name);
        }
        return res;
    }
}
