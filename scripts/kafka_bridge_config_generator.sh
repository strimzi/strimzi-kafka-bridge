#!/usr/bin/env bash

SECURITY_PROTOCOL=PLAINTEXT

if [ "$KAFKA_BRIDGE_TLS" = "true" ]; then
    SECURITY_PROTOCOL="SSL"

    if [ -n "$KAFKA_BRIDGE_TRUSTED_CERTS" ]; then
        TLS_CONFIGURATION=$(cat <<EOF
# TLS / SSL
ssl.truststore.location=/tmp/kafka/bridge.truststore.p12
ssl.truststore.password=${CERTS_STORE_PASSWORD}
ssl.truststore.type=PKCS12
EOF
)
    fi

    if [ -n "$KAFKA_BRIDGE_TLS_AUTH_CERT" ]; then
        TLS_AUTH_CONFIGURATION=$(cat <<EOF
ssl.keystore.location=/tmp/kafka/bridge.keystore.p12
ssl.keystore.password=${CERTS_STORE_PASSWORD}
ssl.keystore.type=PKCS12
EOF
)
    fi
fi

if [ -n "$KAFKA_BRIDGE_SASL_USERNAME" ] && [ -n "$KAFKA_BRIDGE_SASL_PASSWORD_FILE" ]; then
    if [ "$SECURITY_PROTOCOL" = "SSL" ]; then
        SECURITY_PROTOCOL="SASL_SSL"
    else
        SECURITY_PROTOCOL="SASL_PLAINTEXT"
    fi

    PASSWORD=$(cat /opt/kafka/bridge-password/$KAFKA_BRIDGE_SASL_PASSWORD_FILE)

    if [ "x$KAFKA_BRIDGE_SASL_MECHANISM" = "xplain" ]; then
        SASL_MECHANISM="PLAIN"
        JAAS_SECURITY_MODULE="plain.PlainLoginModule"
    elif [ "x$KAFKA_BRIDGE_SASL_MECHANISM" = "xscram-sha-512" ]; then
        SASL_MECHANISM="SCRAM-SHA-512"
        JAAS_SECURITY_MODULE="scram.ScramLoginModule"
    fi

    SASL_AUTH_CONFIGURATION=$(cat <<EOF
sasl.mechanism=${SASL_MECHANISM}
sasl.jaas.config=org.apache.kafka.common.security.${JAAS_SECURITY_MODULE} required username="${KAFKA_BRIDGE_SASL_USERNAME}" password="${PASSWORD}";
EOF
)
fi

# Write the config file
cat <<EOF
#Apache Kafka common
kafka.bootstrapServers=${KAFKA_BRIDGE_BOOTSTRAP_SERVERS}

#Apache Kafka producer
kafka.producer.acks=${KAFKA_BRIDGE_PRODUCER_ACKS}

#Apache Kafka consumer
kafka.consumer.autoOffsetReset=${KAFKA_BRIDGE_CONSUMER_AUTO_OFFSET_RESET}

#AMQP related settings
amqp.enabled=${KAFKA_BRIDGE_AMQP_ENABLED}
amqp.flowCredit=${KAFKA_BRIDGE_AMQP_FLOW_CREDIT}
amqp.mode=${KAFKA_BRIDGE_AMQP_MODE}
amqp.host=${KAFKA_BRIDGE_AMQP_HOST}
amqp.port=${KAFKA_BRIDGE_AMQP_PORT}
amqp.certDir=${KAFKA_BRIDGE_AMQP_CERT_DIR}
amqp.messageConverter=${KAFKA_BRIDGE_AMQP_MESSAGE_CONNVERTER}

#HTTP related settings
http.enabled=${KAFKA_BRIDGE_HTTP_ENABLED}
http.host=${KAFKA_BRIDGE_HTTP_HOST}
http.port=${KAFKA_BRIDGE_HTTP_PORT}

security.protocol=${SECURITY_PROTOCOL}
${TLS_CONFIGURATION}
${TLS_AUTH_CONFIGURATION}
${SASL_AUTH_CONFIGURATION}


EOF