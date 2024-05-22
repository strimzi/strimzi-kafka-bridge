#!/usr/bin/env bash
set -e
set +x

# Clean-up /tmp directory from files which might have remained from previous container restart
# We ignore any errors which might be caused by files injected by different agents which we do not have the rights to delete
rm -rfv /tmp/* || true

MYPATH="$(dirname "$0")"

# Generate temporary keystore password
export CERTS_STORE_PASSWORD=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32)

# Create dir where keystores and truststores will be stored
mkdir -p /tmp/strimzi

# Import certificates into keystore and truststore
"${MYPATH}"/kafka_bridge_tls_prepare_certificates.sh

# Generate and print the bridge config file
echo "Kafka Bridge configuration:"
"${MYPATH}"/kafka_bridge_config_generator.sh | tee /tmp/kafka-bridge.properties | sed 's/sasl.jaas.config=.*/sasl.jaas.config=[hidden]/g' | sed 's/password=.*/password=[hidden]/g'
echo ""

# Configure logging for Kubernetes deployments
export KAFKA_BRIDGE_LOG4J_OPTS="-Dlog4j2.configurationFile=file:$STRIMZI_HOME/custom-config/log4j2.properties"

# Configure Memory
. "${MYPATH}"/dynamic_resources.sh

MAX_HEAP=$(get_heap_size)
if [ -n "$MAX_HEAP" ]; then
  echo "Configuring Java heap: -Xms${MAX_HEAP}m -Xmx${MAX_HEAP}m"
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

if [ -n "$STRIMZI_JAVA_SYSTEM_PROPERTIES" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${STRIMZI_JAVA_SYSTEM_PROPERTIES}"
fi

if [ -n "$STRIMZI_JAVA_OPTS" ]; then
    export JAVA_OPTS="${JAVA_OPTS} ${STRIMZI_JAVA_OPTS}"
fi

# Disable FIPS if needed
if [ "$FIPS_MODE" = "disabled" ]; then
    export JAVA_OPTS="${JAVA_OPTS} -Dcom.redhat.fips=false"
fi

# starting Kafka Bridge with final configuration
exec /usr/bin/tini -s -w -e 143 -- "${MYPATH}"/../kafka_bridge_run.sh --config-file=/tmp/kafka-bridge.properties "$@"
