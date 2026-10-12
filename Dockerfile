#####
# Stage 1: Establish clean ubi9-micro rootfs with its own RPM database
#####
FROM registry.access.redhat.com/ubi9/ubi-micro:latest AS base

#####
# Stage 2: Install runtime dependencies into the ubi9-micro rootfs using dnf --installroot
#####
FROM registry.access.redhat.com/ubi9/ubi-minimal:latest AS builder

ARG JAVA_VERSION=21
ARG TARGETPLATFORM

# Copy the ubi9-micro rootfs (including its RPM database) as the install root
COPY --from=base / /mnt/rootfs/

RUN microdnf install \
        --installroot /mnt/rootfs \
        --noplugins \
        --config /etc/dnf/dnf.conf \
        --setopt=cachedir=/var/cache/microdnf \
        --setopt=reposdir=/etc/yum.repos.d \
        --setopt=varsdir=/etc/dnf \
        --setopt=install_weak_deps=0 \
        --setopt=tsflags=nodocs \
        --releasever 9 \
        -y \
        # Java runtime
        java-${JAVA_VERSION}-openjdk-headless \
        # Required for TLS operations
        openssl \
        # Required for entrypoint scripts
        bash \
        # Required for timezone configuration
        tzdata \
    && microdnf \
        --installroot /mnt/rootfs \
        --noplugins \
        --config /etc/dnf/dnf.conf \
        --setopt=cachedir=/var/cache/microdnf \
        --setopt=reposdir=/etc/yum.repos.d \
        --setopt=varsdir=/etc/dnf \
        clean all

#####
# Get Tini
#####
ENV TINI_VERSION=v0.19.0
ENV TINI_SHA256_AMD64=93dcc18adc78c65a028a84799ecf8ad40c936fdfc5f2a57b1acda5a8117fa82c
ENV TINI_SHA256_ARM64=07952557df20bfd2a95f9bef198b445e006171969499a1d361bd9e6f8e5e0e81
ENV TINI_SHA256_PPC64LE=3f658420974768e40810001a038c29d003728c5fe86da211cff5059e48cfdfde
ENV TINI_SHA256_S390X=931b70a182af879ca249ae9de87ef68423121b38d235c78997fafc680ceab32d

RUN set -ex; \
    mkdir -p /mnt/rootfs/usr/bin; \
    if [[ ${TARGETPLATFORM} = "linux/ppc64le" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-ppc64le -o /mnt/rootfs/usr/bin/tini; \
        echo "${TINI_SHA256_PPC64LE} */mnt/rootfs/usr/bin/tini" | sha256sum -c; \
        chmod +x /mnt/rootfs/usr/bin/tini; \
    elif [[ ${TARGETPLATFORM} = "linux/arm64" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-arm64 -o /mnt/rootfs/usr/bin/tini; \
        echo "${TINI_SHA256_ARM64} */mnt/rootfs/usr/bin/tini" | sha256sum -c; \
        chmod +x /mnt/rootfs/usr/bin/tini; \
    elif [[ ${TARGETPLATFORM} = "linux/s390x" ]]; then \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-s390x -o /mnt/rootfs/usr/bin/tini; \
        echo "${TINI_SHA256_S390X} */mnt/rootfs/usr/bin/tini" | sha256sum -c; \
        chmod +x /mnt/rootfs/usr/bin/tini; \
    else \
        curl -s -L https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -o /mnt/rootfs/usr/bin/tini; \
        echo "${TINI_SHA256_AMD64} */mnt/rootfs/usr/bin/tini" | sha256sum -c; \
        chmod +x /mnt/rootfs/usr/bin/tini; \
    fi

#####
# Stage 3: Build the final container image on ubi9-micro base
#####
FROM registry.access.redhat.com/ubi9/ubi-micro:latest

ARG JAVA_VERSION=21

USER root

#####
# Copy installed packages and Tini from builder chroot (includes updated RPM database)
#####
COPY --from=builder /mnt/rootfs /

# Set JAVA_HOME env var
ENV JAVA_HOME=/usr/lib/jvm/jre-${JAVA_VERSION}

# Add strimzi user with UID 1001
# The user is in the group 0 to have access to the mounted volumes and storage
RUN echo "strimzi:x:1001:0::/home/strimzi:/bin/false" >> /etc/passwd

ARG strimzi_kafka_bridge_version=1.0-SNAPSHOT
ENV STRIMZI_KAFKA_BRIDGE_VERSION=${strimzi_kafka_bridge_version}
ENV STRIMZI_HOME=/opt/strimzi
RUN mkdir -p ${STRIMZI_HOME}
WORKDIR ${STRIMZI_HOME}

COPY target/kafka-bridge-${strimzi_kafka_bridge_version}/kafka-bridge-${strimzi_kafka_bridge_version} ./

USER 1001

CMD ["/opt/strimzi/bin/kafka_bridge_run.sh"]
