[![Build Status](https://dev.azure.com/cncf/strimzi/_apis/build/status/strimzi-kafka-bridge?branchName=main)](https://dev.azure.com/cncf/strimzi/_build/latest?definitionId=34&branchName=main)
[![GitHub release](https://img.shields.io/github/release/strimzi/strimzi-kafka-bridge.svg)](https://github.com/strimzi/strimzi-kafka-bridge/releases/latest)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.strimzi/kafka-bridge/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.strimzi/kafka-bridge)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Twitter Follow](https://img.shields.io/twitter/follow/strimziio?style=social)](https://twitter.com/strimziio)

# HTTP bridge for Apache Kafka®

This project provides a software component which acts as a bridge between [HTTP 1.1 (Hypertext Transfer Protocol)](https://tools.ietf.org/html/rfc2616) and an [Apache Kafka®](https://kafka.apache.org/) cluster.

It provides a different way to interact with Apache Kafka because the latter natively supports only a custom (proprietary) protocol.
Thanks to the bridge, all clients which can speak HTTP 1.1 protocol can connect to Apache Kafka cluster in order to send and receive messages to / from topics.

## Running the bridge

### On Kubernetes and OpenShift

Use the [Strimzi Kafka operator](https://strimzi.io/docs/operators/latest/deploying.html) to deploy the Kafka Bridge with HTTP support on Kubernetes and OpenShift.

### On bare-metal / VM

Download the ZIP or TAR.GZ file from the [GitHub release page](https://github.com/strimzi/strimzi-kafka-bridge/releases) and unpack it.
Afterwards, edit the `config/application.properties` file which contains the configuration.
Once your configuration is ready, start the bridge using:

```bash
bin/kafka_bridge_run.sh --config-file config/application.properties
```

Kafka Bridge releases are also available to [download](https://strimzi.io/downloads/) from the Strimzi website.

## Kafka Bridge documentation

The [Kafka Bridge documentation](https://strimzi.io/docs/bridge/latest/) provides information on getting started with the Kafka Bridge.

The source files for the Kafka Bridge documentation are maintained in the [`documentation`](/documentation) folder.
The documentation is maintained in `.adoc` files.
The `.adoc` files for the API reference section (`documentation/book/api`) are generated from descriptions in the source OpenAPI JSON file (`src/main/resources/openapiv2.json`), so they must not be edited directly.

If you spot something that needs updating or changing in the Kafka Bridge documentation, you can open an issue or open a PR and contribute directly. 

For more information on contributing to the Strimzi documentation, see the [Strimzi Documentation Contributor Guide](https://strimzi.io/contributing/guide/).

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

The [Building Strimzi Kafka Bridge](BUILDING.md) guide describes how to build Strimzi Kafka Bridge and how to test your changes before submitting a patch or opening a PR.

If you want to get in touch with us first before contributing, you can use:

- [Strimzi Dev mailing list](https://lists.cncf.io/g/cncf-strimzi-dev/topics)
- [#strimzi channel on CNCF Slack](https://slack.cncf.io/)

Learn more on how you can contribute on our [Join Us](https://strimzi.io/join-us/) page.

## License

Strimzi Kafka Bridge is licensed under the [Apache License](./LICENSE), Version 2.0

## Container signatures

From the 0.27.0 release, Strimzi Kafka Bridge containers are signed using the [`cosign` tool](https://github.com/sigstore/cosign).
Strimzi currently does not use the keyless signing and the transparency log.
To verify the container, you can copy the following public key into a file:

```
-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAET3OleLR7h0JqatY2KkECXhA9ZAkC
TRnbE23Wb5AzJPnpevvQ1QUEQQ5h/I4GobB7/jkGfqYkt6Ct5WOU2cc6HQ==
-----END PUBLIC KEY-----
```

And use it to verify the signature:

```
cosign verify --key strimzi.pub quay.io/strimzi/kafka-bridge:latest --insecure-ignore-tlog=true
```

## Software Bill of Materials (SBOM)

From the 0.27.0 release, Strimzi Kafka Bridge publishes the software bill of materials (SBOM) of our containers.
The SBOMs are published as an archive with `SPDX-JSON` and `Syft-Table` formats signed using cosign.
For releases, they are also pushed into the container registry.
To verify the SBOM signatures, please use the Strimzi public key:

```
-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAET3OleLR7h0JqatY2KkECXhA9ZAkC
TRnbE23Wb5AzJPnpevvQ1QUEQQ5h/I4GobB7/jkGfqYkt6Ct5WOU2cc6HQ==
-----END PUBLIC KEY-----
```

You can use it to verify the signature of the SBOM files with the following command:

```
cosign verify-blob --key cosign.pub --bundle <SBOM-file>.bundle --insecure-ignore-tlog=true <SBOM-file>
```
