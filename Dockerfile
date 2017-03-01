FROM openjdk:8-jre-alpine

ADD target/amqp-kafka-bridge-1.0-SNAPSHOT.jar /
COPY ./run_bridge.sh /etc/amqp-kafka-bridge/

EXPOSE 5672 5672

CMD ["/etc/amqp-kafka-bridge/run_bridge.sh"]
