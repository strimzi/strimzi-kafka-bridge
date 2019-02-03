FROM centos:7

RUN yum -y install java-1.8.0-openjdk-devel && yum clean all
ENV JAVA_HOME /usr/lib/jvm/java

ARG version=1.0-SNAPSHOT
ADD target/kafka-bridge-${version}-bin.tar.gz /

# copy scripts for starting the bridge
COPY scripts/ /

EXPOSE 5672 5671 8080

CMD ["/run_bridge.sh"]
