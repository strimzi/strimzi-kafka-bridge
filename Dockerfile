FROM openjdk:8-jre-alpine

ARG version=1.0-SNAPSHOT
ADD target/amqp-kafka-bridge-${version}-bin.tar.gz /

EXPOSE 5672 8080

CMD ["/run_bridge.sh"]
