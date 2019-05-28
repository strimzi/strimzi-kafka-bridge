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


# Disable Kafka's GC logging (which logs to a file)...
export GC_LOG_ENABLED="false"

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
export KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$KAFKA_HOME/custom-config/log4j.properties"
fi

# We don't need LOG_DIR because we write no log files, but setting it to a
# directory avoids trying to create it (and logging a permission denied error)
export LOG_DIR="$KAFKA_HOME"

# enabling Prometheus JMX exporter as Java agent
if [ "$KAFKA_BRIDGE_METRICS_ENABLED" = "true" ]; then
  export KAFKA_OPTS="-javaagent:/opt/prometheus/jmx_prometheus_javaagent.jar=9404:$KAFKA_HOME/custom-config/metrics-config.yml"
fi

if [ -z "$KAFKA_HEAP_OPTS" -a -n "${DYNAMIC_HEAP_FRACTION}" ]; then
    . ./dynamic_resources.sh
    # Calculate a max heap size based some DYNAMIC_HEAP_FRACTION of the heap
    # available to a jvm using 100% of the GCroup-aware memory
    # up to some optional DYNAMIC_HEAP_MAX
    CALC_MAX_HEAP=$(get_heap_size ${DYNAMIC_HEAP_FRACTION} ${DYNAMIC_HEAP_MAX})
    if [ -n "$CALC_MAX_HEAP" ]; then
      export KAFKA_HEAP_OPTS="-Xms${CALC_MAX_HEAP} -Xmx${CALC_MAX_HEAP}"
    fi
fi


. ./set_kafka_gc_options.sh

# starting Kafka Bridge with final configuration
exec java -Dvertx.cacheDirBase=/tmp -jar /kafka-bridge.jar --config-file=/tmp/kafka-bridge.properties
