The AMQP protocol has a well defined data type system and metadata for describing content other than an opaque body, which can be encoded using the same AMQP data type system or handled as raw data bytes.
Apache Kafka doesn't have such rich features on transferring messages which are handled as raw bytes.

In order to translate AMQP messages between AMQP client and Apache Kafka, a `MessageConverter` interface is defined with following two methods :

* _toKafkaRecord_ : handles the conversion between an AMQP message to Kafka record;
* _toAmqpMessage_ : translated a Kafka record to an AMQP messages;

The message converter is pluggable through the `message.convert` property inside the bridge configuration file (bridge.properties).

The bridge provides a `DefaultMessageConverter` (as default) and pluggable `JsonMessageConverter` and `RawMessageConverter` converters.

From a Kafka point of view all records (produced and consumed) are defined with a `String` for the key and a `byte[]` array for the value.

## DefaultMessageConverter

It's the simplest converter which works in the following way.

From AMQP message to Kafka record.

* All properties, application properties, message annotations are lost. They are not encoded in any way in the Kafka record;
* If _partition_ and _key_ are specified as message annotations, they are get in order to specify partition and key for topic destination in the Kafka record;
* The AMQP body always handled as bytes and put inside the Kafka record value. The converter supports AMQP value and raw data/binary encoding;

From Kafka record to AMQP message.

* No properties, application properties, message annotations are generated/filled;
* Only _partition_, _offset_ and _key_ message annotations are filled from the Kafka record related information;
* The AMQP body is encoded as raw data/binary from the corresponding Kafka record value;

## JsonMessageConverter

This converter translates and brings all main AMQP message information/metadata/body in a JSON format and it works in the following way.

From AMQP message to Kafka record.

The converter generated a JSON document with following structure :

* All main properties (messageId, to, subject, ...) are converted in a JSON map named _"properties"_ with property name/property value pairs;
* All application properties are converted in a JSON map named _"applicationProperties"_ with property name/property value pairs;
* All message annotations are converted in a JSON map named _"messageAnnotations"_ with annotation name/annotation value pairs. If _partition_ and _key_ are specified as message annotations, they are get in order to specify partition and key for topic destination in the Kafka record;
* The body is encoded in a JSON map named _"body"_ with a _"type"_ field which specifies if it's AMQP value or raw data encoded and a _"section"_ field containing the body content. A raw data bytes section is Base64 encoded;

From Kafka record to AMQP message.

Starting from the received JSON document fro Kafka record it produce an AMQP message in the following way :

* All main properties (messageId, to, subject, ...) are filled from the corresponding JSON map named _"properties"_;
* All application properties are filled from the corresponding JSOM map (if present) named _"applicationProperties"_;
* All message annotations are filled from the corresponding JSON map (if present) named _"messageAnnotations"_. The annotations related to _partition_, _offset_ and _key_ will be always filled;
* The body is encoded as AMQP value or raw data bytes as specified by the _"type"_ field of _"body"_ and the content is get from the _"section"_ field;

```
{
   "properties": {
      "to": ...
      "messageId": ...
      "subject": ...
      "replyTo": ...
      "correlationId": ...
   }
   "applicationProperties": {
      "prop1": ...
      ...
      "propN": ...
   }
   "messageAnnotations": {
      "partition": ...
      "offset": ...
      "key": ...
      "ann1": ...
      ...
      "annN": ...
   }
   "body": {
      "type": ...
      "section": ...
   }
}

```

## RawMessageConverter

This converter doesn't apply any real conversion and works in the following way.

From AMQP message to Kafka record :

* If _partition_ and _key_ are specified as message annotations, they are get in order to specify partition and key for topic destination in the Kafka record;
* The message is encoded as raw bytes in order to put them inside value of Kafka record;

From Kafka record to AMQP message :

* The message is decoded from raw bytes which represents the value inside the Kafka record;
* The annotations related to _partition_, _offset_ and _key_ are filled;


