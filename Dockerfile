FROM centos:7

RUN yum -y install java-1.8.0-openjdk-devel && yum clean all
ENV JAVA_HOME /usr/lib/jvm/java

ARG version=1.0-SNAPSHOT
ADD target/amqp-kafka-bridge-${version}-bin.tar.gz /

EXPOSE 5672 8080

CMD ["/run_bridge.sh"]
