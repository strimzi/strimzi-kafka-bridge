#!/bin/sh

exec java -Dvertx.cacheDirBase=/tmp -cp "libs/*" io.strimzi.kafka.bridge.Application "$@"