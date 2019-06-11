#!/usr/bin/env bash
set +x

MYPATH="$(dirname "$0")"

if [ -n "$KAFKA_BRIDGE_TRUSTED_CERTS" ]; then
    # Generate temporary keystore password
    export CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)

    mkdir -p /tmp/strimzi

    # Import certificates into keystore and truststore
    # $1 = trusted certs, $2 = TLS auth cert, $3 = TLS auth key, $4 = truststore path, $5 = keystore path, $6 = certs and key path
    ${MYPATH}/kafka_bridge_tls_prepare_certificates.sh \
        "$KAFKA_BRIDGE_TRUSTED_CERTS" \
        "$KAFKA_BRIDGE_TLS_AUTH_CERT" \
        "$KAFKA_BRIDGE_TLS_AUTH_KEY" \
        "/tmp/strimzi/bridge.truststore.p12" \
        "/tmp/strimzi/bridge.keystore.p12" \
        "${STRIMZI_HOME}/bridge-certs"
fi

# Generate and print the consumer config file
echo "Kafka Bridge configuration:"
${MYPATH}/kafka_bridge_config_generator.sh | tee /tmp/kafka-bridge.properties
echo ""

# Configure logging for Kubernetes deployments
KAFKA_BRIDGE_LOG4J_OPTS="-Dlog4j.configuration=file:$STRIMZI_HOME/custom-config/log4j.properties"

# Configure Memory
. ${MYPATH}/dynamic_resources.sh

MAX_HEAP=`get_heap_size`
if [ -n "$MAX_HEAP" ]; then
  export JAVA_OPTS="-Xms${MAX_HEAP}m -Xmx${MAX_HEAP}m $JAVA_OPTS"
fi

export MALLOC_ARENA_MAX=2

# Configure GC logging for memory tracking
function get_gc_opts {
  if [ "${STRIMZI_GC_LOG_ENABLED}" == "true" ]; then
    # The first segment of the version number is '1' for releases < 9; then '9', '10', '11', ...
    JAVA_MAJOR_VERSION=$(java -version 2>&1 | sed -E -n 's/.* version "([0-9]*).*$/\1/p')
    if [ "$JAVA_MAJOR_VERSION" -ge "9" ] ; then
      echo "-Xlog:gc*:stdout:time -XX:NativeMemoryTracking=summary"
    else
      echo "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:NativeMemoryTracking=summary"
    fi
  else
    # no gc options
    echo ""
  fi
}

export JAVA_OPTS="${JAVA_OPTS} $(get_gc_opts)"

# starting Kafka Bridge with final configuration
${MYPATH}/../kafka_bridge_run.sh --config-file=/tmp/kafka-bridge.properties "$@"
