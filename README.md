[![Build Status](https://dev.azure.com/cncf/strimzi/_apis/build/status/strimzi-kafka-bridge?branchName=main)](https://dev.azure.com/cncf/strimzi/_build/latest?definitionId=34&branchName=main)
[![GitHub release](https://img.shields.io/github/release/strimzi/strimzi-kafka-bridge.svg)](https://github.com/strimzi/strimzi-kafka-bridge/releases/latest)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.strimzi/kafka-bridge/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.strimzi/kafka-bridge)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Twitter Follow](https://img.shields.io/twitter/follow/strimziio.svg?style=social&label=Follow&style=for-the-badge)](https://twitter.com/strimziio)

# HTTP & AMQP bridge for Apache Kafka®

This project provides a software component which acts as a bridge between different standard protocols and an [Apache Kafka®](https://kafka.apache.org/) cluster.
The available protocols are:

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
bin/kafka_bridge_run.sh --config-file config/application.properties
```

## Documentation

Documentation to the current _main_ branch as well as all releases can be found on our [website](https://strimzi.io).

## Getting help

If you encounter any issues while using Strimzi Kafka Bridge, you can get help through the following methods:

- [Strimzi Users mailing list](https://lists.cncf.io/g/cncf-strimzi-users/topics)
- [#strimzi channel on CNCF Slack](https://slack.cncf.io/)


## Contributing

You can contribute by:
- Raising any issues you find using Strimzi Kafka Bridge
- Fixing issues by opening Pull Requests
- Improving documentation
- Talking about Strimzi Kafka Bridge

All bugs, tasks or enhancements are tracked as [GitHub issues](https://github.com/strimzi/strimzi-kafka-bridge/issues). Issues which
might be a good start for new contributors are marked with ["good-start"](https://github.com/strimzi/strimzi-kafka-bridge/labels/good-start) label.

The [Hacking guide](https://github.com/strimzi/strimzi-kafka-bridge/blob/main/HACKING.md) describes how to build Strimzi Kafka Bridge and how to test your changes before submitting a patch or opening a PR.

The [Documentation Contributor Guide](http://strimzi.io/contributing/guide/) describes how to contribute to Strimzi documentation.

If you want to get in touch with us first before contributing, you can use:

- [Strimzi Dev mailing list](https://lists.cncf.io/g/cncf-strimzi-dev/topics)
- [#strimzi channel on CNCF Slack](https://slack.cncf.io/)

## License

Strimzi Kafka Bridge is licensed under the [Apache License](./LICENSE), Version 2.0
