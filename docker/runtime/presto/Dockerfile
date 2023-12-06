ARG BASE_TAG="nightly"
FROM cloudtik/cloudtik-deps:$BASE_TAG

ARG NODEX_VERSION=1.6.1
ARG PROMETHEUS_VERSION=2.45.0
ARG PRESTO_VERSION=0.276

ENV RUNTIME_PATH /home/cloudtik/runtime
RUN mkdir -p $RUNTIME_PATH
WORKDIR /home/cloudtik/runtime

# Install prometheus and nodex
ENV NODEX_HOME $RUNTIME_PATH/nodex
ENV PROMETHEUS_HOME $RUNTIME_PATH/prometheus

RUN wget https://github.com/prometheus/node_exporter/releases/download/v${NODEX_VERSION}/node_exporter-${NODEX_VERSION}.linux-amd64.tar.gz -O nodex.tar.gz && \
    mkdir -p "$NODEX_HOME" && \
    tar --extract --file nodex.tar.gz --directory "$NODEX_HOME" --strip-components 1 --no-same-owner && \
    mv $NODEX_HOME/node_exporter $NODEX_HOME/nodex && \
    rm nodex.tar.gz && \
    wget https://github.com/prometheus/prometheus/releases/download/v${PROMETHEUS_VERSION}/prometheus-${PROMETHEUS_VERSION}.linux-amd64.tar.gz -O prometheus.tar.gz && \
    mkdir -p "$PROMETHEUS_HOME" && \
    tar --extract --file prometheus.tar.gz --directory "$PROMETHEUS_HOME" --strip-components 1 --no-same-owner && \
    rm prometheus.tar.gz

# Install JDK
ENV JAVA_HOME            $RUNTIME_PATH/jdk
ENV PATH                 $JAVA_HOME/bin:$PATH

# JDK download links refer to https://github.com/adoptium/containers
# and https://github.com/docker-library/docs/blob/master/eclipse-temurin/README.md
RUN wget https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.16.1%2B1/OpenJDK11U-jdk_x64_linux_hotspot_11.0.16.1_1.tar.gz -O openjdk.tar.gz && \
    mkdir -p "$JAVA_HOME" && \
    tar --extract --file openjdk.tar.gz --directory "$JAVA_HOME" --strip-components 1 --no-same-owner && \
    rm openjdk.tar.gz


# Install Presto
ENV PRESTO_VERSION        ${PRESTO_VERSION}
ENV PRESTO_HOME           $RUNTIME_PATH/presto
ENV PATH                  $PRESTO_HOME/bin:$PATH

RUN wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz -O presto-server.tar.gz \
    && mkdir -p "$PRESTO_HOME" \
    && tar --extract --file presto-server.tar.gz --directory "$PRESTO_HOME" --strip-components 1 --no-same-owner \
    && rm presto-server.tar.gz \
    && wget https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar \
    && mv presto-cli-${PRESTO_VERSION}-executable.jar $PRESTO_HOME/bin/presto \
    && chmod +x $PRESTO_HOME/bin/presto

WORKDIR /home/cloudtik/
