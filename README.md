# AMQP - Apache Kafka bridge

This project provides a software component which acts as a bridge between [AMQP (Advanced Message Queuing Protocol)](https://www.amqp.org/) based clients and [Apache Kafka](http://kafka.apache.org/) cluster.

It provides a different way to interact with Apache Kafka because the latter supports natively only custom (proprietary) protocol. Thanks to the bridge, all clients which can speak AMQP (which is a standard OASIS and ISO-IEC) protocol can connect to Apache Kafka cluser in order to send and receive messages to/from topics.

## Bridge library

The project leverages on [Vert.x](http://vertx.io/) framework and uses the [Vert.x Proton](https://github.com/vert-x3/vertx-proton) library in order to provide the AMQP server for accepting and handling connections, sessions and links.

All the bridge related classes are defined inside the _io.ppatierno.kafka.bridge_ package and the only class needed to instantiate the bridge is the _Bridge_ class which needs a _Vertx_ instance too.

Other than core classes, the following stuff is provided :

* BridgeTest : a bunch of unit testing classes based on JUnit and Vert.x Unit;
* BridgeServer : a sample application which instantiates and starts the bridge;
* BridgeReceiver : a sample applcation with a bunch of examples using AMQP receivers;
* BridgeSender : a sample application with a bunch of examples using AMQP senders;

## Server application

This is a simple application which implements a server running the bridge. It can starts in the two following ways :

* no parameter : a default bridge configuration will be used;
* configuration file path : the bridge will load configuration from that (i.e. bridge.properties);

## Docker image

This is a simple Docker image which containes the server application as a fat JAR with all dependencies related to Vert.x Proton and the AMQP - Kafka Bridge.

## AMQP Senders

In order to send a message to a topic named _[topic]_, an AMQP client should attach a link on the following simple address :

_[topic]_ 

or specifying it inside the _"To"_ system properties of the AMQP message itself.

The AMQP message body contains the Apache Kafka message the client wants to send to the topic. Other than the body, the client can add the following message annotations in order to specify the topic partition destination :

* _x-opt-bridge.partition_ : the topic partition destination on which the message should be published;
* _x-opt-bridge.key_ : the key used to determine the topic partition destination (instead of the previous partition annotation);

The above message annotations aren't mandatory. If they aren't provided by the clients, the messages are sent across all topic partitions in a round robin fashion.

More information about sender flow are available in the wiki [here](https://github.com/ppatierno/amqp-kafka-bridge/wiki/Sender)

## AMQP Receivers

In order to consume messages from a topic named _[topic]_, an AMQP client should attach a link on the address with following format :

_[topic]/group.id/[group.id]_

where _[group.id]_ is the consumer group identifier as needed by Apache Kafka.
The returned AMQP message contains the Apache Kafka provided message inside its body adding the following message annotations :

* _x-opt-bridge.partition_ : the topic partition on which the message was read;
* _x-opt-bridge.offset_ : the message offset inside the partition;
* _x-opt-bridge.key_ : the message key (used to determine the partition at sending time);

## Supported AMQP clients

All AMQP based clients are supported and can be used in order to connect to the bridge for sending/receiving messages to/from Apache Kafka topics.
The main well known AMQP projects are :

* [Apache Qpid](https://qpid.apache.org/) : provides an AMQP stack implementation in C, Java, C++ and other languages. Other than clients, a [Dispatch Router](https://qpid.apache.org/components/dispatch-router/index.html) is available as well;
* [AMQP .Net Lite](https://github.com/Azure/amqpnetlite) : .Net and C# based implementation of the AMQP stack;
