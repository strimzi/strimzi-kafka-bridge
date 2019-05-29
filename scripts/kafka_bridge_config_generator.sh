#!/usr/bin/env bash

# Write the config file

SECURITY_PROTOCOL=PLAINTEXT

if [ "$KAFKA_BRIDGE_TLS" = "true" ]; then
    SECURITY_PROTOCOL="SSL"

    if [ -n "$KAFKA_BRIDGE_TRUSTED_CERTS" ]; then
        TLS_CONFIGURATION=$(cat <<EOF
# TLS / SSL
kafka.ssl.truststore.location=/tmp/kafka/bridge.truststore.p12
kafka.ssl.truststore.password=${CERTS_STORE_PASSWORD}
kafka.ssl.truststore.type=PKCS12
EOF
)
    fi

    if [ -n "$KAFKA_BRIDGE_TLS_AUTH_CERT" ] && [ -n "$KAFKA_BRIDGE_TLS_AUTH_KEY" ]; then
        TLS_AUTH_CONFIGURATION=$(cat <<EOF
kafka.ssl.keystore.location=/tmp/kafka/bridge.keystore.p12
kafka.ssl.keystore.password=${CERTS_STORE_PASSWORD}
kafka.ssl.keystore.type=PKCS12
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

    PASSWORD=$(cat /opt/bridge/bridge-password/$KAFKA_BRIDGE_SASL_PASSWORD_FILE)

    if [ "x$KAFKA_BRIDGE_SASL_MECHANISM" = "xplain" ]; then
        SASL_MECHANISM="PLAIN"
        JAAS_SECURITY_MODULE="plain.PlainLoginModule"
    elif [ "x$KAFKA_BRIDGE_SASL_MECHANISM" = "xscram-sha-512" ]; then
        SASL_MECHANISM="SCRAM-SHA-512"
        JAAS_SECURITY_MODULE="scram.ScramLoginModule"
    fi

    SASL_AUTH_CONFIGURATION=$(cat <<EOF
kafka.sasl.mechanism=${SASL_MECHANISM}
kafka.sasl.jaas.config=org.apache.kafka.common.security.${JAAS_SECURITY_MODULE} required username="${KAFKA_BRIDGE_SASL_USERNAME}" password="${PASSWORD}";
EOF
)
fi

content="#Common properties\n"
content="${content}kafka.security.protocol=${SECURITY_PROTOCOL}"
content="${content}${TLS_CONFIGURATION}"
content="${content}${TLS_AUTH_CONFIGURATION}"
content="${content}${SASL_AUTH_CONFIGURATION}"
content="$content\n"
content="${content}kafka.bootstrap.servers=${KAFKA_BRIDGE_BOOTSTRAP_SERVERS}\n"

content="$content\n"

content="${content}#Apache Kafka Producer\n"

for i in $KAFKA_BRIDGE_PRODUCER_CONFIG; do
	content="${content}kafka.producer."
	content="${content}$(echo -n $i | cut -d'=' -f1)"
	content="$content="
	content="$content$(echo -n $i | cut -d'=' -f2)"
	content="$content\n"
done

content="$content\n"
content="$content#Apache Kafka Consumer\n"
for i in $KAFKA_BRIDGE_CONSUMER_CONFIG; do
	content="${content}kafka.consumer."
	content="$content$(echo -n $i | cut -d'=' -f1)"
	content="$content="
	content="$content$(echo -n $i | cut -d'=' -f2)"
	content="$content\n"
done

content="$content\n"
content="$content#HTTP\n"
content="${content}http.enabled=$KAFKA_BRIDGE_HTTP_ENABLED\n"
content="${content}http.host=$KAFKA_BRIDGE_HTTP_HOST\n"
content="${content}http.port=$KAFKA_BRIDGE_HTTP_PORT\n"

content="$content\n"
content="$content#AMQP\n"
content="${content}amqp.enabled=$KAFKA_BRIDGE_AMQP_ENABLED\n"
content="${content}amqp.host=$KAFKA_BRIDGE_AMQP_HOST\n"
content="${content}amqp.port=$KAFKA_BRIDGE_AMQP_PORT\n"
content="${content}amqp.mode=$KAFKA_BRIDGE_AMQP_MODE\n"
content="${content}amqp.flowCredit=$KAFKA_BRIDGE_AMQP_FLOW_CREDIT\n"
content="${content}amqp.certDir=$KAFKA_BRIDGE_AMQP_CERT_DIR\n"
content="${content}amqp.messageConverter=$KAFKA_BRIDGE_AMQP_MESSAGE_CONVERTER\n"

echo -e $content