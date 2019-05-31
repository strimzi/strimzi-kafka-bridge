#!/bin/sh

MYPATH="$(dirname "$0")"

exec java -Dvertx.cacheDirBase=/tmp -cp "${MYPATH}/../libs/*" io.strimzi.kafka.bridge.Application "$@"