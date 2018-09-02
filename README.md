[![Build Status](https://travis-ci.org/strimzi/strimzi-kafka-bridge.svg?branch=master)](https://travis-ci.org/strimzi/strimzi-kafka-bridge)

# Apache Kafka bridge

This project provides a software component which acts as a bridge between different standard protocols and an [Apache Kafka](http://kafka.apache.org/) cluster.
The current supported protocols are :

* [AMQP 1.0 (Advanced Message Queuing Protocol)](https://www.amqp.org/)
* [HTTP 1.1 (Hypertext Transfer Protocol)](https://tools.ietf.org/html/rfc2616)

It provides a different way to interact with Apache Kafka because the latter supports natively only custom (proprietary) protocol. Thanks to the bridge, all clients which can speak different standard protocols can connect to Apache Kafka cluster in order to send and receive messages to/from topics.

## Internals

You can find more documentation on "internals" and how the bridge works at following [doc](doc/README.md) folder.
