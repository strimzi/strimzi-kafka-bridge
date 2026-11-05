/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge.auth;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * Represents the username and password extracted from the Authorization header
 * (Basic scheme) of an HTTP request, passed through to the internal Kafka clients.
 * The credentials are not validated by the bridge: the Kafka cluster is responsible for it.
 */
public class BasicAuthCredentials implements AuthCredentials {

    private final String username;
    private final String password;

    /**
     * Constructor
     *
     * @param username username coming from the HTTP request
     * @param password password coming from the HTTP request
     */
    public BasicAuthCredentials(String username, String password) {
        this.username = username;
        this.password = password;
    }

    /**
     * Create the credentials from the Base64-encoded value of the Authorization header
     * (Basic scheme): the decoded value must be in the "username:password" format
     * (the password may contain colons, the split is done on the first colon)
     *
     * @param base64Credentials Base64-encoded credentials from the Authorization header
     * @return the credentials, null if the value is not valid Base64 or not in the expected format
     */
    public static BasicAuthCredentials fromBase64(String base64Credentials) {
        if (base64Credentials == null) {
            return null;
        }
        String decoded;
        try {
            decoded = new String(Base64.getDecoder().decode(base64Credentials), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return null;
        }
        // split on the first colon, because the password may contain colons
        int separator = decoded.indexOf(':');
        if (separator <= 0) {
            return null;
        }
        return new BasicAuthCredentials(decoded.substring(0, separator), decoded.substring(separator + 1));
    }

    /**
     * Get the username.
     *
     * @return the username
     */
    public String username() {
        return this.username;
    }

    /**
     * Get the password.
     *
     * @return the password
     */
    public String password() {
        return this.password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BasicAuthCredentials that)) {
            return false;
        }
        return Objects.equals(this.username, that.username)
                && Objects.equals(this.password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.username, this.password);
    }

    @Override
    public String toString() {
        return "BasicAuthCredentials(username=" + this.username + ", password=***masked***)";
    }
}
