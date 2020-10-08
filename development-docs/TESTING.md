# Testing Kafka Bridge

This document gives a detailed breakdown of the testing processes and testing options for `Kafka Bridge` within tests.

<!-- TOC depthFrom:2 -->

- [Pre-requisites](#pre-requisites)
- [Package Structure](#package-structure)
- [Approaches](#approaches)
- [Test Phases](#test-phases)
- [Available Test Groups](#available-test-groups)
- [Environment Variables](#environment-variables)
- [Running single test class](#running-single-test-class)

<!-- /TOC -->

## Pre-requisites

To run any tests you need to build whole project. You can achieve that with following command - `mvn clean install -DskipTests` 
in root directory of the project.

The next requirement is to have [docker](https://docs.docker.com/get-docker/) installed, because we use test-containers. 
In case of interest you can check our implementation of Strimzi `Kafka` container [here](https://github.com/strimzi/strimzi-kafka-operator/tree/master/test-container).

## Package Structure

You can find tests inside `src.test.java` package. Moreover we have the auxiliary classes and the most notable one are:
 
- `Utils` static methods
- `Clients` clients for testing overall communication of the `Kafka Bridge`
- `Facades` encapsulation of the standalone `Kafka` and `AdminClient` instance

Furthermore we divide our test suite on two separate parts:

1. `Http`package, where you can find all test cases related to the `Http Kafka Bridge`
2. `Amqp`package, where you can find all tests cases related to the `Amqp Kafka Bridge`

## Approaches

There are four approaches how you can verify that `Kafka Bridge` is without any bug. Before I list all approaches 
the following  terminology is needed.

#### In-memory 
`Kafka` = 
- for `Http Kafka Bridge` we use [Strimzi Kafka container]([here](https://github.com/strimzi/strimzi-kafka-operator/tree/master/test-container).)
- for `Amqp Kafka Bridge` we use [KafkaFacade](https://github.com/strimzi/strimzi-kafka-bridge/blob/master/src/test/java/io/strimzi/kafka/bridge/facades/KafkaFacade.java) 

`Kafka Bridge` =
 - for deployment of the `Http Kafka Bridge` and `Amqp Kafka Bridge` we are using [Vert.x](https://vertx.io/) 

#### Standalone 
`Kafka` 
 - we use latest version of the `Kafka` and you can start the `Kafka` instance by the `zookeeper-server-start.sh` and
`kafka-server-start.sh`, which can be for instance downloaded from [this](https://kafka.apache.org/downloads) site

`Kafka Bridge` 
 - start `Kafka Bridge` instance using bash script located `target/kafka-bridge-{version}/kafka-bridge-{version}/bin/kafka_bridge_run.sh` 
with following command `kafka_bridge_run.sh --config-file ../config/application.properties`
 
----  
The approaches are as follows:

|  Approach	| Supported |
|---	|---	| 
|   1. Using in-memory `Kafka` and in-memory `Kafka Bridge` | Yes
|   2. Using standalone `Kafka` and standalone `Kafka Bridge` | Yes
|   3. Using in-memory `Kafka` and standalone `Kafka Bridge` | No
|   4. Using standalone `Kafka` and in-memory `Kafka Bridge` | No

In our CI we are currently testing the first approach which is the most important. Also, in the future we are planning 
to add the second option. Rest of these approaches are not so essential. 

## Test Phases

In general, we use classic test phases: `setup`, `exercise`, `verify` and `teardown`. Every phase will be described for the
`src.test.java.io.strimzi.kafka.bridge.http` package. It is very similar with `src.test.java.io.strimzi.kafka.bridge.amqp` only
one difference is that `http` package using Strimzi `Kafka` container by `test-containers` and `amqp` package using KafkaFacade.

### Setup

In this phase we perform:

* Deploy the in-memory Strimzi `Kafka` container or standalone `Kafka`
* Deploy the AdminClient
* Deploy the in-memory `Kafka Bridge` or standalone `Kafka-Bridge`
* Deploy the WebClient

Everything is setup in the `HttpBridgeTestBase` class. In the static block we instantiate the Strimzi `Kafka` container,
AdminClient to be shared across all test suites. In `@Before` we create our `Kafka` client, `Kafka Bridge` instance and lastly
Webclient, which is used for communication with `Kafka Bridge` REST API.

Setup in-memory Strimzi `Kafka` container and in-memory `Kafka Bridge` example:
```
    ...
    ...
    static {
        kafkaContainer = new StrimziKafkaContainer();        
        kafkaContainer.start();                                 <--- deploy in-memory Strimzi Kafka container

        kafkaUri = kafkaContainer.getBootstrapServers();        <--- get Kafka bootstrap address

        adminClientFacade = AdminClientFacade.create(kafkaUri); <--- deploy in-memory AdminClient
    }
    ...
    ...
    @BeforeAll
    void setup() {
    
        basicKafkaClient = new BasicKafkaClient(kafkaUri);  <--- setup Kafka client
        bridgeConfig = BridgeConfig.fromMap(config);        <--- setup bridge config
        
        httpBridge = new HttpBridge(bridgeConfig, new MetricsReporter(jmxCollectorRegistry, meterRegistry)); <--- new instance of Kafka Bridge
        httpBridge.setHealthChecker(new HealthChecker());

        LOGGER.info("Deploying in-memory bridge");
        vertx.deployVerticle(httpBridge, context.succeeding(id -> context.completeNow())); <--- deploy in-memory Kafka Bridge
    
        client = WebClient.create(vertx, new WebClientOptions() <--- webclient for communication with REST API of Kafka Bridge
            .setDefaultHost(Urls.BRIDGE_HOST)
            .setDefaultPort(Urls.BRIDGE_PORT));
}
```

In case testing standalone Strimzi `Kafka` container it is excepted that you have already spin-up your Kafka cluster locally. 
Same is applied on standalone `Kafka Bridge` instance. Note that you have to have set the environment variables `BRIDGE_EXTERNAL_ENV` and
`KAFKA_EXTERNAL_ENV`. Semantics of these variables is described in section Environment variables.

### Exercise

In this phase you specify all steps which you need to execute to cover some specific functionality.

### Verify

When your environment is in place from the previous phase, you can add code for some checks, msg exchange, etc.

### Teardown

In teardown phase we perform deletion of the `Kafka Bridge` instance. What is worth mentioning is that Strimzi `Kafka` container is
implicitly stopped because of static block.

Teardown is triggered in `@AfterAll` of `HttpBridgeTestBase`:
```
    @AfterAll
    static void afterAll(VertxTestContext context) {
        if ("FALSE".equals(BRIDGE_EXTERNAL_ENV)) {                              <--- checking if external Kafka Bridge should run
            vertx.close(context.succeeding(arg -> context.completeNow()));      <--- closing the vertx instance
        } else {
            // if we running external bridge
            context.completeNow();
        }
    }
```

## Available Test groups

You need to use the `groups` system property in order to execute a group of tests. For example with the following values:

`-Dgroups=httpbridge` — to execute one test group
`-Dgroups=httpbridge,amqpbridge` — to execute many test groups
`-Dgroups=all` — to execute all test groups

If `-Dgroups` system property isn't defined, all tests without an explicitly declared test group will be executed.
The following table shows currently used tags:

| Name               | Description                                                                        |
| :----------------: | :--------------------------------------------------------------------------------: |
| httpbridge         | Http Bridge tests, which guarantee, that functionality of Http Bridge is working.  |
| amqpbridge         | Amqp Bridge tests, which guarantee, that functionality of Amqp Bridge is working.  |

There is also a mvn profile for the main groups - `httpbridge`, `amqpbridge` and `all`, but we suggest to use profile with id `all` (default) and then include or exclude specific groups.
If you want specify the profile, use the `-P` flag - for example `-Phttpbridge`.

All available test groups are listed in [Constants](https://github.com/strimzi/strimzi-kafka-bridge/blob/master/src/test/java/io/strimzi/kafka/bridge/Constants.java) class.

## Environment variables

System tests can be configured by several environment variables, which are loaded before test execution.

| Name                                   | Description                                                                              | Default                                         |
| :------------------------------------: | :--------------------------------------------------------------------------------------: | :----------------------------------------------:|
| KAFKA_EXTERNAL_ENV                     |  Specify if tests should run against in-memory or external Kafka cluster                 | false                                           |
| BRIDGE_EXTERNAL_ENV                    | Specify if tests should run against in-memory or external Kafka Bridge                   | false                                           |

## Running single test class

Use the `verify` build goal and provide `-Dit.test=TestClassName[#testMethodName]` system property.

    mvn verify -Dit.test=ConsumerTest#receiveSimpleMessage
