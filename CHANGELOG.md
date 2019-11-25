# CHANGELOG

## 0.16.0

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
