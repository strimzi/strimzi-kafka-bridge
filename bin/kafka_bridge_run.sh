#!/bin/sh

# Find my path to use when calling scripts
MYPATH="$(dirname "$0")"

# Configure logging
if [ -z "$KAFKA_BRIDGE_LOG4J_OPTS" ]
then
      KAFKA_BRIDGE_LOG4J_OPTS="-Dlog4j2.configurationFile=file:${MYPATH}/../config/log4j2.properties"
fi

# Make sure that we use /dev/urandom
JAVA_OPTS="${JAVA_OPTS} -Dvertx.cacheDirBase=/tmp -Djava.security.egd=file:/dev/./urandom"

if [[ "$@ " =~ "--docker " ]]; then
    echo "Running with tini"
    exec /usr/bin/tini -s -w -e 143 -- java $JAVA_OPTS $KAFKA_BRIDGE_LOG4J_OPTS -classpath "${MYPATH}/../libs/*" io.strimzi.kafka.bridge.Application "$@"
else
    java $JAVA_OPTS $KAFKA_BRIDGE_LOG4J_OPTS -classpath "${MYPATH}/../libs/*" io.strimzi.kafka.bridge.Application "$@"
fi