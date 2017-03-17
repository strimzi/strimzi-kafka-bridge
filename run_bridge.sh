#!/bin/sh

if [ -z "$KAFKA_BOOTSTRAP_SERVERS" ]; then
    if [ -n "$KAFKA_SERVICE_HOST" ]; then
        export KAFKA_BOOTSTRAP_SERVERS=$KAFKA_SERVICE_HOST:$KAFKA_SERVICE_PORT
    else
        echo "ERROR: Kafka bootstrap servers not configured"
    fi
fi

# configuring the bridge to work in "client" mode connecting to the messaging (router) layer
export AMQP_MODE="CLIENT"
export AMQP_HOST=$MESSAGING_SERVICE_HOST
export AMQP_PORT=$MESSAGING_SERVICE_PORT_INTERNAL

exec java -Dvertx.disableFileCaching=true -Dvertx.disableFileCPResolving=true -jar /amqp-kafka-bridge-1.0-SNAPSHOT.jar
