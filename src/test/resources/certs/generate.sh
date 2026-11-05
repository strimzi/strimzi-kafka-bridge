#!/usr/bin/env bash
#
# Generates the certificate material used by the TLS-related tests:
# the TLS client certificate authentication (ClientCertAuthIT), the OIDC TLS listener
# (OidcTlsListenerIT) and the provider mTLS tests (OidcTokenValidatorTest).
#
# All certificates are signed by the self-signed "Test CA" (EC P-256, 100 years validity).
# The bridge/broker certificates carry the localhost SANs and the server/client EKUs;
# the client certificates carry the clientAuth EKU. The "client-expired" certificate has
# an allowed subject (CN=allowed-user) but a validity period fully in the past, to test
# that the validity period is enforced by the TLS chain validation (the handshake fails
# even though the subject would be allowed).
#
# Re-running the script regenerates the whole suite with a fresh CA: the embedded
# certificate constants (CA, bridge certificate and key) in ClientCertAuthIT must then
# be updated accordingly.
#
# Note: the CA private key is not kept - regenerate everything with this script instead.

set -euo pipefail
cd "$(dirname "$0")"

rm -f *.csr *.srl ca.key broker.key broker.crt *.p12

# --- Certification authority (self-signed, 100 years) -----------------------------------------
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout ca.key -out ca.crt -days 36500 -subj "/CN=Test CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign"

# --- Bridge certificate (CN=bridge, SANs for localhost, server + client EKUs) ------------------
openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout bridge-pkcs8.key -out bridge.csr -subj "/CN=bridge"
openssl x509 -req -in bridge.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out bridge.crt -days 36500 -sha256 \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth")

# --- Kafka broker certificate (CN=broker-0, SANs for localhost, server EKU) --------------------
openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout broker.key -out broker.csr -subj "/CN=broker-0"
openssl x509 -req -in broker.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out broker.crt -days 36500 -sha256 \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth")

# --- Client certificates (clientAuth EKU only) -------------------------------------------------
for client in "allowed:CN=allowed-user" "forbidden:CN=forbidden-user"; do
    name="${client%%:*}"
    subject="${client#*:}"
    openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
        -keyout "client-${name}.key" -out "client-${name}.csr" -subj "/${subject}"
    openssl x509 -req -in "client-${name}.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
        -out "client-${name}.crt" -days 36500 -sha256 -extfile <(printf "extendedKeyUsage=clientAuth")
done

# expired client certificate: allowed subject, validity period fully in the past
openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -keyout client-expired.key -out client-expired.csr -subj "/CN=allowed-user"
openssl x509 -req -in client-expired.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out client-expired.crt -sha256 -extfile <(printf "extendedKeyUsage=clientAuth") \
    -not_before 20260101000000Z -not_after 20260601000000Z

# --- Keystores for the Kafka broker (identity + truststore for the bridge certificate CA) ------
openssl pkcs12 -export -inkey broker.key -in broker.crt -certfile ca.crt \
    -name "1" -out server.keystore.p12 -passout pass:certs-password
keytool -importcert -file ca.crt -keystore client.truststore.p12 -storetype PKCS12 \
    -storepass certs-password -alias test-ca -noprompt

# --- Clean up the intermediates (including the CA private key) ---------------------------------
rm -f *.csr *.srl ca.key broker.key broker.crt
