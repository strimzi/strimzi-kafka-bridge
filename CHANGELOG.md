# CHANGELOG

## 0.20.0

* Added a new Admin Client feature to get begin/end offsets for topic partitions

## 0.19.0

* Fixed bug not allowing to send records with `null` values
* Added support for Kafka headers on records to send
* Refactoring around overall tests suite
* Fixed some other minor bugs

## 0.18.0

* Use Java 11 as the Java runtime
* Renamed exposed HTTP server and Kafka consumer and producer metrics, using `strimzi_bridge` as prefix
* Added topic-partition retrivial operation to the Admin Client endpoint

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
