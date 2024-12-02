# CHANGELOG

## 0.31.1

* Fixed missing SCRAM-SHA-256 support.

## 0.31.0

* Dependency updates (Kafka 3.9.0, Vert.x 4.5.11, Netty 4.1.115.Final)
* Added support for creating a new topic via endpoint.
* Fixed metadata order on the HTTP "offsets" response when producing records.

## 0.30.0

* Dependency updates (Kafka 3.8.0, Vert.x 4.5.9, Netty 4.1.111.Final, Prometheus JMX Collector 1.0.1, Prometheus Client 1.3.1)
* Added support for message timestamp.
  * Setting the timestamp on a message sent via the `send` API.
  * Getting the timestamp on receiving a message via the `poll` API.

## 0.29.0

* Dependency updates (Vert.x 4.5.8, Netty 4.1.110.Final)
* Added support for records key and value to be JSON array when using the `json` embedded format.
* Update the base image used by Strimzi containers from UBI8 to UBI9
* The OpenAPI v2 (Swagger) specification support is now deprecated.
  * The `/openapi` endpoint still returns the OpenAPI v2 specification. There is an additional `/openapi/v2` endpoint returning the same.
  * The newly added `/openapi/v3` endpoint returns the OpenAPI v3 specification. Please use this one, because the v2 will be removed in the future.

## 0.28.0

* Dependency updates (Kafka 3.7.0, Kubernetes configuration provider 1.1.2, Vert.x 4.5.4, Netty 4.1.107.Final, Jackson FasterXML 2.16.1, Micrometer 1.12.3, OAuth 0.15.0, OpenTelemetry 1.34.1, OpenTelemetry Semconv 1.21.0-alpha, OpenTelemetry instrumentation 1.32.0-alpha)
* Fixed missing messaging semantic attributes to the Kafka consumer spans
* Introduced a new text embedded format to send and receive plain strings for record key and value.
* Removed the dependency on OkHttp and thus Kotlin.
* This release deprecates several attributes (inline with changes from OpenTelemetry Semconv) which it attaches to spans. Both the deprecated attribute and its replacement will be added to spans in the current release. The deprecated attributes will be removed in a future version.
  1. `http.method` is being replaced with `http.request.method`
  2. `http.url` is being replaced with `url.scheme`, `url.path` & `url.query`
  3. `messaging.destination` is being replaced with `messaging.destination.name`
  4. `http.status_code` is being replaced with `http.response.status_code`
* The span attribute `messaging.destination.kind=topic` is deprecated, by OpenTelemetry Semconv, without a replacement. It will be removed in a future release of the Strimzi Kafka Bridge.

## 0.27.0

* Removed support for OpenTracing:
  * The `bridge.tracing=jaeger` configuration is not valid anymore.
  * The OpenTelemetry based tracing is the only available by using `bridge.tracing=opentelemetry`.
* Fixed logging Kafka TLS related password for trust/key stores on startup.
* Sign containers using cosign
* Generate and publish Software Bill of Materials (SBOMs) of Strimzi containers
* Dependency updates (Kafka 3.6.0, OAuth 0.14.0)
* Dependency updates (Vert.x 4.4.6, Netty 4.1.100.Final [CVE-2023-44487](https://nvd.nist.gov/vuln/detail/CVE-2023-44487))

## 0.26.1

* Dependency updates (Kafka 3.5.1 [CVE-2023-34455](https://nvd.nist.gov/vuln/detail/CVE-2023-34455))

## 0.26.0

* Removed "remote" and "local" labels from HTTP server related metrics to avoid a big growth of time series samples.
* Removed accounting HTTP server metrics for requests on the `/metrics` endpoint.
* Exposed the `/metrics` endpoint through the OpenAPI specification.
* Fixed OpenAPI 3.0 `OffsetRecordSentList` component schema returning proper record offsets or error.
* Fixed OpenAPI `ConsumerRecord` component schema returning key and value not only as (JSON) string but even as object.
* Fixed OpenAPI HTTP status codes returned by `/ready` and `/healthy`:
  * from 200 to 204 because no content in the response in case of success.
  * added 500 in the specification for the failure case, as already returned by the bridge.
* Dependency updates (Vert.x 4.4.4, Netty 4.1.94.Final to align with Vert.x, Kafka 3.5.0, snakeYAML 2.0, JMX Prometheus Exporter 0.18.0, Jackson 2.14.2, OAuth 0.13.0)

## 0.25.0

* Fixed printing operation log at the proper logging level.
* Fixed sending messages with headers to the `sendToPartition` endpoint.
* Added missing validation on the `async` query parameter for sending operations in the OpenAPI v3 specification.
* Fixed memory calculation by using JVM so taking cgroupv2 into account.
* Dependency updates (Vert.x 4.3.8, Netty 4.1.87 to align with Vert.x, Kafka 3.4.0, OpenTelemetry 1.19.0)
* Added feature to unsubscribe topics by executing a subscribe or assign request with an empty topics list.
* Strimzi OAuth updated to 0.12.0 with support for automatic retries during authentication and token validation.

## 0.24.0

* Fixed regression about producing messages not working when CORS is enabled
* Dependency updates (Netty 4.1.86.Final)
* Refactored producer, consumer and admin endpoints by using the Apache Kafka client API instead of the Vert.x one
* Removed the usage of the Vert.x Config component for reading the bridge configuration
* Replaced the Vert.x JSON handling with Jackson

### Changes, deprecations and removals

Support for `bridge.tracing=jaeger` tracing based on Jaeger clients and OpenTracing API was deprecated in the 0.22.0 release.
As the Jaeger clients are retired and the OpenTracing project is archived, we cannot guarantee their support for future versions.
In the 0.22.0 release, we added support for OpenTelemetry tracing as a replacement.
If possible, we will maintain the support for `bridge.tracing=jaeger` tracing until June 2023 and remove it afterwards.
Please migrate to OpenTelemetry as soon as possible.

## 0.23.1

* Dependency updates (Netty 4.1.85.Final)

## 0.23.0

* Moved from using the Jaeger exporter to OTLP exporter by default
* Moved to Java 11 at language level, dropped Java 8 and use Java 17 as the runtime for all containers
* Fixed bug about missing OAuth password grants configuration   
* Dependency updates (Vert.x 4.3.5)
* Dependency updates (snakeYAML [CVE-2022-41854](https://nvd.nist.gov/vuln/detail/CVE-2022-41854))
* Add Oauth metrics into the `jmx_metrics_config.yaml`

### Changes, deprecations and removals

* Since this version there is no support for AMQP 1.0 protocol anymore. 
  This was discussed and approved with the proposal [#42](https://github.com/strimzi/proposals/blob/main/042-remove-bridge-amqp-support.md)

## 0.22.3

* Strimzi OAuth updated to 0.11.0 bringing support for password grants and OAuth metrics
* Fixed bug about tracing span creation missing when the consumer poll operation fails
* OpenTelemetry updated to 1.18.0
* Dependency updates (Jackson Databind) 
  * [CVE-2022-42003](https://nvd.nist.gov/vuln/detail/CVE-2022-42003)
  * [CVE-2022-42004](https://nvd.nist.gov/vuln/detail/CVE-2022-42004)

## 0.22.2

* Enable rack-awareness when the bridge is deployed by the Strimzi operator
* Documentation improvements
  * How to enable distributed tracing
  * More details about using Cross-Origin Resource Sharing (CORS)
* Dependency updates (Kafka [CVE-2022-34917](https://nvd.nist.gov/vuln/detail/CVE-2022-34917))
* Dependency updates (Netty)

## 0.22.1

* Dependency updates (snakeYAML [CVE-2022-38752](https://nvd.nist.gov/vuln/detail/CVE-2022-38752))
* Dependency updates (Kubernetes configuration provider)

## 0.22.0

* Add OpenTelemetry support for tracing
  * Enabling Jaeger exporter by default for backward compatibility with OpenTracing support

### Changes, deprecations and removals

* Since the OpenTracing project was archived, the related support in the bridge is now deprecated.

## 0.21.6

* Add `async` query parameter to publish endpoint to allow for immediate responses from the bridge (reduces latency)
* Dependency updates (Vert.x 4.3.1, Apache Kafka 3.2.0, etc.)
* Documentation improvements

## 0.21.5

* Support for ppc64le platform
* Documentation improvements
* Dependency updates

## 0.21.4

* Dependency updates (Configuration providers, Vert.x, Netty, Oauth client and more)
* Add support for disabling the FIPS mode in OpenJDK
* Add transactions `isolation.level` configuration parameter on consumer creation
* Support for s390x platform

## 0.21.3

* Dependency updates (Log4j2 [CVE-2021-44832](https://nvd.nist.gov/vuln/detail/CVE-2021-44832))

## 0.21.2

* Dependency updates (Log4j2 [CVE-2021-45105](https://nvd.nist.gov/vuln/detail/CVE-2021-45105))

## 0.21.1

* Dependency updates (Log4j2 [CVE-2021-45046](https://nvd.nist.gov/vuln/detail/CVE-2021-45046))
* Fix parsing of provided JVM options when deployed by Strimzi Cluster Operator

## 0.21.0

* Dependency updates (Log4j2 [CVE-2021-44228](https://cve.mitre.org/cgi-bin/cvename.cgi?name=CVE-2021-44228))
* Multi-arch container image with support for x86_64 / AMD64 and AArch64 / ARM64 platforms
  _(The support AArch64 is currently considered as experimental. We are not aware of any issues, but the AArch64 build doesn't at this point undergo the same level of testing as the AMD64 container images.)_

## 0.20.0

* Added a new Admin Client feature to get begin/end offsets for topic partitions
* Move from Docker Hub to Quay.io as our container registry
* Use Red Hat UBI8 as the base image

## 0.19.0

* Fixed bug not allowing to send records with `null` values
* Added support for Kafka headers on records to send
* Refactoring around overall tests suite
* Fixed some other minor bugs

## 0.18.0

* Use Java 11 as the Java runtime
* Renamed exposed HTTP server and Kafka consumer and producer metrics, using `strimzi_bridge` as prefix
* Added topic-partition retrieval operation to the Admin Client endpoint

## 0.17.0

* Added an Admin Client endpoint and related API for getting topics list and topic details
* Made available bridge information (i.e. version) on the root `/` endpoint
* Exposed HTTP server, JVM and Kafka consumer and producer related metrics in the Prometheus format
* Added tini usage for running the bridge in containerized environment
* Use `log4j2` instead of `log4j`

## 0.16.0

* Add support for CORS

## 0.15.0

* Added support for Jaeger tracing
* Various bug fixes.

## 0.14.0

* Changed data types and enforced OpenAPI validation for `consumer.request.timeout.ms`, `enable.auto.commit`and `fetch.min.bytes`parameters on consumer creation. This is a breaking change.
* Added HTTP GET method on `/consumers/{groupid}/instances/{name}/subscription` endpoint for getting subscribed topics and related assigned partitions.
* Added automatic deletion of stale consumer after a configurable timeout if the HTTP DELETE is not called and the consumer is not used for long time.
* Add support for OAuth authentication for connections between the Bridge and Kafka brokers (not HTTP connections)
* Various bug fixes.

## 0.13.0

* Exposed "healthy" and "ready" endpoints as part of the OpenAPI specification
* Added support for handling API gateway/proxy path routing to the bridge
* Fixed OpenAPI v2 validation errors

## 0.12.0

* First release of the HTTP bridge
