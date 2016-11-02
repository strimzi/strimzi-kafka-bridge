FROM java:8u72

MAINTAINER Paolo Patierno <ppatierno@live.com>

RUN mkdir /amqp-kafka-bridge
COPY ./amqp-kafka-server-0.0.1-SNAPSHOT.jar /amqp-kafka-bridge
COPY ./bridge.properties /amqp-kafka-bridge
WORKDIR /amqp-kafka-bridge

EXPOSE 5672

CMD ["java", "-jar", "/amqp-kafka-bridge/amqp-kafka-server-0.0.1-SNAPSHOT.jar"]
