# Testing Kafka Bridge

This document gives a detailed breakdown of the testing processes and testing options for `Kafka Bridge` within tests.

<!-- TOC depthFrom:2 -->

- [Pre-requisites](#pre-requisites)
- [Package Structure](#package-structure)
- [Kafka and Bridge installation methods](#kafka-and-bridge-installation-methods)
- [Test Phases](#test-phases)
- [Available Test Groups](#available-test-groups)
- [Environment Variables](#environment-variables)
- [Running single test class](#running-single-test-class)

<!-- /TOC -->

## Pre-requisites

To run any tests you need to build whole project. You can achieve that with following command - `mvn clean install -DskipTests` 
in root directory of the project.

The next requirement is to have [docker](https://docs.docker.com/get-docker/) installed, because we use [test-containers](https://www.testcontainers.org/). 
In case of interest you can check our implementation of Strimzi `Kafka` container [here](https://github.com/strimzi/strimzi-kafka-operator/tree/main/test-container).

> The `test-containers` project uses [Moby Ryuk](https://github.com/testcontainers/moby-ryuk) to automatic cleanup of containers after the execution. Ryuk must be started as a privileged container.
Running in privileged mode might not apply for all environments by default.  If you get an error like `java.lang.IllegalArgumentException: Requested port (8080) is not mapped` 
when `test-containers` tries to start, you can set `TESTCONTAINERS_RYUK_CONTAINER_PRIVILEGED=true` or disable Ryuk by setting `TESTCONTAINERS_RYUK_DISABLED=true`. 

## Package Structure

You can find tests inside `src.test.java` package. Moreover we have the auxiliary classes and the most notable one are:
 
- `utils` - static methods
- `clients` - clients for testing overall communication of the `Kafka Bridge`
- `facades` - encapsulation of the standalone `Kafka` and `AdminClient` instance
- `configuration` - annotations used in the tests for configuring Bridge cluster
- `enums` - enums used in tests
- `extensions` - test extensions for deploying Kafka, Bridge and for creating object storing all test related variables
- `httpclient` - HTTP service with utils methods for communicating with Bridge via HTTP requests
- `objects` - objects used in tests - mainly for creating objects for easier manipulation in tests from JSON objects (using Jackson)

All test cases related to the `Http Kafka Bridge` are then stored inside `http` package.

## Kafka and Bridge installation methods

The Kafka cluster is always deployed using [strimzi-test-containers](https://github.com/strimzi/test-container).
Based on the `RUN_WITH_ALL_KAFKA_VERSIONS` environment variable, the Bridge tests can be executed against:
- latest Kafka version when set to `false`.
- to list of Kafka versions when set to `true` (default). For the list of Kafka versions we are taking the supported versions by the `strimzi-test-container` and we filter them to use just latest versions for the particular minor version
  (that means if the supported versions are `4.1.0`, `4.1.1`, `4.2.0`, `4.2.1`, `4.2.2` then we take `4.1.1` and `4.2.2`).

In case of Bridge cluster, there are two modes that can be configured using `BRIDGE_RUN_MODE`:
- `IN_MEMORY` - default mode, Bridge is started using VertX verticle. This mode is useful in case of local debugging, you are able to stop at particular place in the code.
- `CONTAINER` - Bridge is started using [test-containers](https://github.com/testcontainers). This mode is useful for testing the image we are producing (the image can be configured using `BRIDGE_IMAGE` env variable).

Currently in the CI we are testing against list of Kafka versions with in-memory Bridge. 
But in the future we should definitely add step for running the very same tests also against container Bridge (and test the image we are producing).

## Test Phases

In general, we use classic test phases: `setup`, `exercise`, `verify` and `teardown`. Every phase will be described for the
`io.strimzi.kafka.bridge.http` package as part of the tests.

### Setup

In this phase we perform:

* Deploy the Strimzi `Kafka` container
* Deploy the in-memory or container of `Kafka Bridge`
* Create Admin client and HTTP Service

Everything is set up in the `BridgeSuite`. 
This annotation contains all extensions needed for deploying everything needed for the tests.
Based on the `TestInstance.Lifecycle` - if `PER_CLASS` or `PER_METHOD` is specified - the Bridge cluster is deployed.
In case that `PER_CLASS` is used (default in the `BridgeSuite` annotation), Bridge is deployed _before all_ tests.
Otherwise, in case that `PER_METHOD` is used (you can overwrite it on class-level), Bridge is deployed _before each_ test. 
Based on the `TestInstance.Lifecycle` you can also configure the Bridge using the `BridgeConfiguration` annotation - there is default used in the `BridgeSuite`, however you can configure the Bridge any way you want and need.
The `BridgeConfiguration` should be added to the particular method in case of `TestInstance.Lifecycle.PER_METHOD`, otherwise it should be added for the whole class.

Setup of the Kafka and Bridge is written as extension and you will not need to bother with writing the _before all/each_ methods in every test class.
However, your test class have to extend the `AbstractIT` class in order to use the `BridgeSuite` with its extensions, but also to provide parameter for the Kafka version, as we are running in the `ParameterizedClass` and
the parameter for Kafka has to be specified.

### Exercise

In this phase you specify all steps which you need to execute to cover some specific functionality.

### Verify

When your environment is in place from the previous phase, you can add code for some checks, msg exchange, etc.

### Teardown

In teardown phase, we are stopping the in-memory or container Bridge based on the `TestInstance.Lifecycle` - so either after each test when `TestInstance.Lifecycle.PER_METHOD` is used, or after all tests when `TestInstance.Lifecycle.PER_CLASS` is used.
The Kafka cluster is stopped after everything.

## Available Test groups

You need to use the `groups` system property in order to execute a group of tests. For example with the following values:

`-Dgroups=httpbridge` — to execute one test group
`-Dgroups=all` — to execute all test groups

If `-Dgroups` system property isn't defined, all tests without an explicitly declared test group will be executed.
The following table shows currently used tags:

| Name               | Description                                                                        |
| :----------------: | :--------------------------------------------------------------------------------: |
| httpbridge         | Http Bridge tests, which guarantee, that functionality of Http Bridge is working.  |

There is also a mvn profile for the main groups - `httpbridge` and `all`, but we suggest to use profile with id `all` (default) and then include or exclude specific groups.
If you want specify the profile, use the `-P` flag - for example `-Phttpbridge`.

All available test groups are listed in [Constants](https://github.com/strimzi/strimzi-kafka-bridge/blob/main/src/test/java/io/strimzi/kafka/bridge/Constants.java) class.

## Environment variables

System tests can be configured by several environment variables, which are loaded before test execution.

|            Name             |                                                                                  Description                                                                                  |               Default               |
|:---------------------------:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|:-----------------------------------:|
| RUN_WITH_ALL_KAFKA_VERSIONS |     Environment variable configuring if the Bridge tests should run against all Kafka versions (latest patch version of each minor Kafka version) or just against latest.     |                true                 |
|       BRIDGE_RUN_MODE       | Configures mode in which we are going to start the Bridge cluster for the tests - IN_MEMORY, CONTAINER. In case of CONTAINER, we are going to test the official Bridge image. |              IN_MEMORY              |
|        BRIDGE_IMAGE         |                             If tests are executed with Bridge running in test-container, this environment is used for configuring the used image                              | quay.io/strimzi/kafka-bridge:latest |


## Running single test class

Use the `test` build goal and provide `-Dtest=TestClassName[#testMethodName]` system property.

    mvn test -Dtest=SeekTest#seekToNotExistingTopic
