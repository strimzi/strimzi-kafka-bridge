#!/usr/bin/env bash
set +x

if [ -n "$KAFKA_BRIDGE_TRUSTED_CERTS" ]; then
    # Generate temporary keystore password
    export CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)

    mkdir -p /tmp/kafka

    # Import certificates into keystore and truststore
    # $1 = trusted certs, $2 = TLS auth cert, $3 = TLS auth key, $4 = truststore path, $5 = keystore path, $6 = certs and key path
    ./kafka_bridge_tls_prepare_certificates.sh \
        "$KAFKA_BRIDGE_TRUSTED_CERTS" \
        "$KAFKA_BRIDGE_TLS_AUTH_CERT" \
        "$KAFKA_BRIDGE_TLS_AUTH_KEY" \
        "/tmp/kafka/bridge.truststore.p12" \
        "/tmp/kafka/bridge.keystore.p12" \
        "/opt/kafka/bridge-certs"
fi

# Generate and print the consumer config file
echo "Kafka Bridge configuration:"
./kafka_bridge_config_generator.sh | tee /tmp/kafka-bridge.properties
echo ""

# starting Kafka Bridge with final configuration
exec java -Dvertx.cacheDirBase=/tmp -jar /kafka-bridge.jar --config-file=/tmp/kafka-bridge.properties
