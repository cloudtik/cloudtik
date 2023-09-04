ARG BASE_IMAGE="nightly"
FROM cloudtik/cloudtik-deps:"$BASE_IMAGE"

ARG PT_VERSION="2.0.1"
ARG VERSION="1.11.0"
ARG REVISION="587"
ARG ARTIFACTORY_URL="vault.habana.ai"

RUN sudo apt-get update && sudo apt-get install -y \
    curl \
    libcurl4 \
    moreutils \
    iproute2 \
    libcairo2-dev \
    libglib2.0-dev \
    libhdf5-dev \
    libselinux1-dev \
    libnuma-dev \
    libpcre2-dev \
    libjpeg-dev \
    liblapack-dev \
    libopenblas-dev \
    numactl \
    libgoogle-perftools-dev && \
    sudo apt-get clean && sudo rm -rf /var/lib/apt/lists/*

ENV PIP_DISABLE_PIP_VERSION_CHECK=1

COPY requirements.txt /tmp/requirements.txt
RUN pip --no-cache-dir install -r /tmp/requirements.txt \
    && sudo rm /tmp/requirements.txt \
    && pip --no-cache-dir install habana_media_loader=="${VERSION}"."${REVISION}"

# PyTorch packages
COPY install_packages.sh .
RUN export PIP_NO_CACHE_DIR=on && \
    /bin/bash ./install_packages.sh && rm -f install_packages.sh && \
    sudo /sbin/ldconfig && echo "source /etc/profile.d/habanalabs.sh" >> ~/.bashrc

ENV LD_PRELOAD=/lib/x86_64-linux-gnu/libtcmalloc.so.4
ENV TCMALLOC_LARGE_ALLOC_REPORT_THRESHOLD=7516192768

RUN rm -rf /tmp/*

ENV AI_WITH_HPU true
