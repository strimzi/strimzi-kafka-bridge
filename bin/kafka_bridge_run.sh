#!/bin/sh
set -x

# Find my path to use when calling scripts
MYPATH="$(dirname "$0")"

# enabling OpenTelemetry with OTLP by default
if [ -n "$OTEL_SERVICE_NAME" ] && [ -z "$OTEL_TRACES_EXPORTER" ]; then
  export OTEL_TRACES_EXPORTER="otlp"
fi

exec java $JAVA_OPTS -jar "${MYPATH}"/../quarkus-run.jar "$@"