/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.http;

import io.strimzi.kafka.bridge.config.AbstractConfig;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * HTTP related configuration
 */
public class HttpConfig extends AbstractConfig {

    /** Prefix for the HTTP protocol related configuration */
    public static final String HTTP_CONFIG_PREFIX = "http.";

    /** Enable CORS on HTTP */
    public static final String HTTP_CORS_ENABLED = HTTP_CONFIG_PREFIX + "cors.enabled";

    /** Allowed origins with CORS */
    public static final String HTTP_CORS_ALLOWED_ORIGINS = HTTP_CONFIG_PREFIX + "cors.allowedOrigins";

    /** Allowed methods with CORS */
    public static final String HTTP_CORS_ALLOWED_METHODS = HTTP_CONFIG_PREFIX + "cors.allowedMethods";

    /** HTTP bridge host address */
    public static final String HTTP_HOST = HTTP_CONFIG_PREFIX + "host";

    /** HTTP bridge port */
    public static final String HTTP_PORT = HTTP_CONFIG_PREFIX + "port";

    /** HTTP bridge port for management endpoints such as /healthy, /ready and /metrics  */
    public static final String MANAGEMENT_PORT = HTTP_CONFIG_PREFIX + "management.port";

    /** Enable SSL connections  */
    public static final String HTTP_SERVER_SSL_ENABLE = HTTP_CONFIG_PREFIX + "ssl.enable";

    /** Comma separated list of protocols enabled for SSL connections */
    public static final String HTTP_SERVER_SSL_ENABLED_PROTOCOLS = HTTP_CONFIG_PREFIX + "ssl.enabled.protocols";

    /** Comma separated list of cipher suites for HTTP Bridge server  */
    public static final String HTTP_SERVER_SSL_ENABLED_CIPHER_SUITES = HTTP_CONFIG_PREFIX + "ssl.enabled.cipher.suites";

    /** HTTP Bridge server certificate file location */
    public static final String HTTP_SERVER_SSL_CERTIFICATE_LOCATION = HTTP_CONFIG_PREFIX + "ssl.certificate.location";
    /** HTTP Bridge server private key file location */
    public static final String HTTP_SERVER_SSL_KEY_LOCATION = HTTP_CONFIG_PREFIX + "ssl.key.location";

    /** HTTP Bridge server certificate */
    public static final String HTTP_SERVER_SSL_CERTIFICATE = HTTP_CONFIG_PREFIX + "ssl.certificate";
    /** HTTP Bridge server private key */
    public static final String HTTP_SERVER_SSL_KEY = HTTP_CONFIG_PREFIX + "ssl.key";

    /** HTTP consumer timeouts */
    public static final String HTTP_CONSUMER_TIMEOUT = HTTP_CONFIG_PREFIX + "timeoutSeconds";
    /** Enable consumer part of the bridge */
    public static final String HTTP_CONSUMER_ENABLED = HTTP_CONFIG_PREFIX + "consumer.enabled";
    /** Enable producer part of the bridge */
    public static final String HTTP_PRODUCER_ENABLED = HTTP_CONFIG_PREFIX + "producer.enabled";

    /** Default HTTP host address if not specified */
    public static final String DEFAULT_HOST = "0.0.0.0";

    /** Default HTTP port if not specified */
    public static final int DEFAULT_PORT = 8080;

    /** Default HTTP port if not specified when SSL is enabled */
    public static final int DEFAULT_HTTPS_PORT = 8443;

    /** Default management port */
    public static final int DEFAULT_MANAGEMENT_PORT = 8081;

    /** Default HTTP consumer timeout if not specified (no timeout) */
    public static final long DEFAULT_CONSUMER_TIMEOUT = -1L;

    /** Default SSL enabled protocols to be used when SSL is enabled */
    public static final String DEFAULT_SSL_ENABLED_PROTOCOLS = "TLSv1.2,TLSv1.3";
    /**
     * Constructor
     *
     * @param config configuration parameters map
     */
    private HttpConfig(Map<String, Object> config) {
        super(config);
    }

    /**
     * Get the host for HTTP server (to bind).
     *
     * @return the host for HTTP server (to bind)
     */
    public String getHost() {
        return (String) this.config.getOrDefault(HTTP_HOST, DEFAULT_HOST);
    }

    /**
     * Get the port for HTTP server (to bind).
     *
     * @return the port for HTTP server (to bind)
     */
    public int getPort() {
        if (isSslEnabled()) {
            return Integer.parseInt(this.config.getOrDefault(HTTP_PORT, DEFAULT_HTTPS_PORT).toString());
        }
        return Integer.parseInt(this.config.getOrDefault(HTTP_PORT, DEFAULT_PORT).toString());
    }

    /**
     * Get the port for HTTP management server (to bind).
     *
     * @return the port for HTTP management server (to bind)
     */
    public int getManagementPort() {
        return Integer.parseInt(this.config.getOrDefault(MANAGEMENT_PORT, DEFAULT_MANAGEMENT_PORT).toString());
    }

    /**
     * Get the timeout for closing inactive consumer.
     *
     * @return the timeout for closing inactive consumer
     */
    public long getConsumerTimeout() {
        return Long.parseLong(this.config.getOrDefault(HTTP_CONSUMER_TIMEOUT, DEFAULT_CONSUMER_TIMEOUT).toString());
    }

    /**
     * Get if CORS is enabled.
     *
     * @return if CORS is enabled
     */
    public boolean isCorsEnabled() {
        return Boolean.parseBoolean(this.config.getOrDefault(HTTP_CORS_ENABLED, false).toString());
    }

    /**
     * Get the list of CORS Allowed Origins.
     *
     * @return list of CORS Allowed Origins (default *)
     */
    public String getCorsAllowedOrigins() {
        return (String) this.config.getOrDefault(HTTP_CORS_ALLOWED_ORIGINS, "*");
    }

    /**
     * Get the list of CORS Allowed Methods.
     *
     * @return list of CORS Allowed Methods (default GET,POST,PUT,DELETE,OPTIONS,PATCH)
     */
    public String getCorsAllowedMethods() {
        return (String) this.config.getOrDefault(HTTP_CORS_ALLOWED_METHODS, "GET,POST,PUT,DELETE,OPTIONS,PATCH");
    }

    /**
     * Get if consumer is enabled.
     *
     * @return if consumer is enabled
     */
    public boolean isConsumerEnabled() {
        return Boolean.parseBoolean(this.config.getOrDefault(HTTP_CONSUMER_ENABLED, "true").toString());
    }

    /**
     * Get if producer is enabled.
     *
     * @return if producer is enabled
     */
    public boolean isProducerEnabled() {
        return Boolean.parseBoolean(this.config.getOrDefault(HTTP_PRODUCER_ENABLED, "true").toString());
    }

    /**
     * Get if SSL is enabled.
     *
     * @return if SSL is enabled
     */
    public Boolean isSslEnabled() {
        return Boolean.parseBoolean(this.config.getOrDefault(HTTP_SERVER_SSL_ENABLE, "false").toString());
    }

    /**
     * Get the set of SSL enabled protocols.
     *
     * @return set of SSL enabled protocols
     */
    public Set<String> getHttpServerSslEnabledProtocols() {
        String protocols = (String) this.config.getOrDefault(HTTP_SERVER_SSL_ENABLED_PROTOCOLS, DEFAULT_SSL_ENABLED_PROTOCOLS);
        if (protocols != null) {
            return Arrays.stream(protocols.split(",")).collect(Collectors.toSet());
        } else {
            return null;
        }
    }

    /**
     * Get the SSL cipher suites.
     *
     * @return SSL cipher suites
     */
    public Set<String> getHttpServerSslCipherSuites() {
        String cipherSuites = (String) this.config.getOrDefault(HTTP_SERVER_SSL_ENABLED_CIPHER_SUITES, null);
        if (cipherSuites != null) {
            return Arrays.stream(cipherSuites.split(",")).collect(Collectors.toSet());
        } else {
            return null;
        }
    }

    /**
     * Get the path to the SSL certificate.
     *
     * @return path to the SSL certificate
     */
    public String getHttpServerSslCertificateLocation() {
        return (String) this.config.getOrDefault(HTTP_SERVER_SSL_CERTIFICATE_LOCATION, null);
    }

    /**
     * Get the path to the SSL private key.
     *
     * @return path to the SSL private key
     */
    public String getHttpServerSslKeyLocation() {
        return (String) this.config.getOrDefault(HTTP_SERVER_SSL_KEY_LOCATION, null);
    }

    /**
     * Get the SSL certificate.
     *
     * @return SSL certificate
     */
    public String getHttpServerSslCertificate() {
        return (String) this.config.getOrDefault(HTTP_SERVER_SSL_CERTIFICATE, null);
    }

    /**
     * Get the SSL private key.
     *
     * @return SSL private key
     */
    public String getHttpServerSslKey() {
        return (String) this.config.getOrDefault(HTTP_SERVER_SSL_KEY, null);
    }


    /**
     * Loads HTTP related configuration parameters from a related map
     *
     * @param map map from which loading configuration parameters
     * @return HTTP related configuration
     */
    public static HttpConfig fromMap(Map<String, Object> map) {
        // filter the HTTP related configuration parameters, stripping the prefix as well
        return new HttpConfig(map.entrySet().stream()
                .filter(e -> e.getKey().startsWith(HttpConfig.HTTP_CONFIG_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    @Override
    public String toString() {
        return "HttpConfig(" +
                "config=" + this.config +
                ")";
    }
}
