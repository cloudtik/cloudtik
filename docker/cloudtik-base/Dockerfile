# The base Docker image installs main libraries needed to run CloudTik

# The GPU options are NVIDIA CUDA developer images.
ARG BASE_IMAGE="ubuntu:focal"
FROM ${BASE_IMAGE}
# FROM directive resets ARG
ARG DEBIAN_FRONTEND=noninteractive
ARG PYTHON_VERSION=3.8
ARG CPU_ARCH=${CPU_ARCH:-x86_64}
ARG CONDA_ENV_NAME=cloudtik
ARG CLOUDTIK_UID=1000
ARG CLOUDTIK_GID=100

ENV TZ=America/Los_Angeles

RUN apt-get update -y \
    && apt-get install -y sudo tzdata \
    && useradd -ms /bin/bash -d /home/cloudtik cloudtik --uid $CLOUDTIK_UID --gid $CLOUDTIK_GID \
    && usermod -aG sudo cloudtik \
    && echo 'cloudtik ALL=NOPASSWD: ALL' >> /etc/sudoers \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

USER $CLOUDTIK_UID

ENV HOME=/home/cloudtik
ENV CONDA_ROOT=$HOME/miniconda
ENV PATH=$CONDA_ROOT/bin:$PATH
ENV CLOUDTIK_ENV=$CONDA_ENV_NAME

SHELL ["/bin/bash", "-c"]
RUN sudo apt-get update -y && sudo apt-get upgrade -y \
    && sudo apt-get install -y \
        vim \
        git \
        wget \
        curl \
        tmux \
        screen \
        rsync \
        openssh-client \
        gnupg \
        jq \
        dnsutils \
        uuid \
        openssh-server \
        apt-utils \
    # Install Conda
    && wget \
        --quiet "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-${CPU_ARCH}.sh" \
        -O /tmp/miniconda.sh \
    && /bin/bash /tmp/miniconda.sh -b -u -p $CONDA_ROOT \
    && $CONDA_ROOT/bin/conda init \
    && echo 'export CONDA_ROOT=$HOME/miniconda' >> $HOME/.bashrc \
    && echo "export CLOUDTIK_ENV=$CLOUDTIK_ENV" >> $HOME/.bashrc \
    && echo 'export PATH=${CONDA_ROOT}/bin:$PATH' >> $HOME/.bashrc \
    && rm /tmp/miniconda.sh \
    # Install kubectl
    && sudo mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.28/deb/Release.key | sudo gpg --yes --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg \
    && echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.28/deb/ /' | sudo tee /etc/apt/sources.list.d/kubernetes.list \
    && sudo apt-get update \
    && sudo apt-get install kubectl \
    # Clean up
    && sudo rm -rf /var/lib/apt/lists/* \
    && sudo rm -rf /etc/apt/sources.list.d/kubernetes.list \
    && sudo apt-get clean

RUN if [[ "${PYTHON_VERSION}" = "3.11" || "${PYTHON_VERSION}" =~ ^3\.11\. ]]; then \
        $CONDA_ROOT/bin/conda create -n $CLOUDTIK_ENV -y python=$PYTHON_VERSION; \
    else \
        # Conda issue #12287: ping libffi=3.3 for python version below 3.11
        # TODO: Remove libffi=3.3 once it is resolved
        $CONDA_ROOT/bin/conda create -n $CLOUDTIK_ENV -y libffi=3.3 python=$PYTHON_VERSION; \
    fi \
    && $CONDA_ROOT/bin/conda clean -itqy

ENV PATH=$CONDA_ROOT/envs/$CLOUDTIK_ENV/bin:$PATH
ENV CLOUDTIK_DOWNLOADS="https://d30257nes7d4fq.cloudfront.net/downloads"

WORKDIR $HOME
