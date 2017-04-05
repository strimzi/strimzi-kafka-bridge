FROM centos:7

RUN yum -y install java-1.8.0-openjdk-devel && yum clean all
ENV JAVA_HOME /usr/lib/jvm/java

ARG version=1.0-SNAPSHOT
ADD target/amqp-kafka-bridge-${version}.jar /
COPY ./run_bridge.sh /etc/amqp-kafka-bridge/

EXPOSE 5672 5672

CMD ["/etc/amqp-kafka-bridge/run_bridge.sh"]
