# CHANGELOG

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
