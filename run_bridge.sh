#!/bin/sh

exec java -Dvertx.disableFileCaching=true -Dvertx.disableFileCPResolving=true -jar /amqp-kafka-bridge-1.0-SNAPSHOT.jar
