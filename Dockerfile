FROM centos:7
ARG JAVA_VERSION=11

RUN yum -y update \
    && yum -y install java-${JAVA_VERSION}-openjdk-headless openssl \
    && yum -y clean all

# Set JAVA_HOME env var
ENV JAVA_HOME /usr/lib/jvm/java

# Add strimzi user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN useradd -r -m -u 1001 -g 0 strimzi

ARG strimzi_kafka_bridge_version=1.0-SNAPSHOT
ENV STRIMZI_KAFKA_BRIDGE_VERSION ${strimzi_kafka_bridge_version}
ENV STRIMZI_HOME=/opt/strimzi
RUN mkdir -p ${STRIMZI_HOME}
WORKDIR ${STRIMZI_HOME}

COPY target/kafka-bridge-${strimzi_kafka_bridge_version}/kafka-bridge-${strimzi_kafka_bridge_version} ./

#####
# Add Tini
#####
ENV TINI_VERSION v0.19.0
ENV TINI_SHA256_AMD64=93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c
ENV TINI_SHA256_PPC64LE=3f658420974768e40810001a038c29d003728c5fe86da211cff5059e48cfdfde

RUN set -ex; \
    if [[ ${TARGETPLATFORM} = "linux/ppc64le" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-ppc64le -o /usr/bin/tini; \
        echo "${TINI_SHA256_PPC64LE} */usr/bin/tini" | sha256sum -c; \
        chmod +x /usr/bin/tini; \
    else \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o /usr/bin/tini; \
        echo "${TINI_SHA256_AMD64} */usr/bin/tini" | sha256sum -c; \
        chmod +x /usr/bin/tini; \
    fi

USER 1001

CMD ["/opt/strimzi/bin/kafka_bridge_run.sh"]
