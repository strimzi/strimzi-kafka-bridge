[![Build Status](https://travis-ci.org/strimzi/strimzi-kafka-bridge.svg?branch=master)](https://travis-ci.org/strimzi/strimzi-kafka-bridge)

# Apache Kafka bridge

This project provides a software component which acts as a bridge between different standard protocols and an [Apache Kafka](http://kafka.apache.org/) cluster.
The current supported protocols are:

* [AMQP 1.0 (Advanced Message Queuing Protocol)](https://www.amqp.org/)
* [HTTP 1.1 (Hypertext Transfer Protocol)](https://tools.ietf.org/html/rfc2616)

It provides a different way to interact with Apache Kafka because the latter natively supports only a custom (proprietary) protocol. 
Thanks to the bridge, all clients which can speak different standard protocols can connect to Apache Kafka cluster in order to send and receive messages to / from topics.

## Running the bridge

### On Kubernetes and OpenShift

You can use the [Strimzi Kafka operator](https://strimzi.io/) to deploy the Kafka Bridge with HTTP support on Kubernetes and OpenShift.

### On bare-metal / VM

Download the ZIP or TAR.GZ file from the [GitHub release page](https://github.com/strimzi/strimzi-kafka-bridge/releases) and unpack it.
Afterwards, edit the `config/application.properties` file which contains the configuration.
Once your configuration is ready, start the bridge using:

```bash
bin/kafka-bridge-run.sh --config-file config/application.properties
```

## Internals

You can find more documentation on "internals" and how the bridge works at following [doc](doc/README.md) folder.
