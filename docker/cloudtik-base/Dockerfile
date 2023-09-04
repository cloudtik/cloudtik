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
ENV PATH="/home/cloudtik/miniconda/bin:$PATH"

RUN apt-get update -y \
    && apt-get install -y sudo tzdata \
    && useradd -ms /bin/bash -d /home/cloudtik cloudtik --uid $CLOUDTIK_UID --gid $CLOUDTIK_GID \
    && usermod -aG sudo cloudtik \
    && echo 'cloudtik ALL=NOPASSWD: ALL' >> /etc/sudoers \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

USER $CLOUDTIK_UID
ENV HOME=/home/cloudtik
ENV CLOUDTIK_ENV=$CONDA_ENV_NAME

SHELL ["/bin/bash", "-c"]
RUN sudo apt-get update -y && sudo apt-get upgrade -y \
    && sudo apt-get install -y \
        vim \
        git \
        wget \
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
    && /bin/bash /tmp/miniconda.sh -b -u -p $HOME/miniconda \
    && $HOME/miniconda/bin/conda init \ 
    && echo 'export CONDA_ROOT=$HOME/miniconda' >> $HOME/.bashrc \
    && echo "export CLOUDTIK_ENV=$CLOUDTIK_ENV" >> $HOME/.bashrc \
    && echo 'export PATH=$HOME/miniconda/bin:$PATH' >> $HOME/.bashrc \
    && rm /tmp/miniconda.sh \
    # Install kubectl
    && wget -O - -q https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add - \
    && sudo touch /etc/apt/sources.list.d/kubernetes.list \
    && echo "deb http://apt.kubernetes.io/ kubernetes-xenial main" | sudo tee -a /etc/apt/sources.list.d/kubernetes.list \
    && sudo apt-get update \
    && sudo apt-get install kubectl \
    # Clean up
    && sudo rm -rf /var/lib/apt/lists/* \
    && sudo rm -rf /etc/apt/sources.list.d/kubernetes.list \
    && sudo apt-get clean

RUN if [[ "${PYTHON_VERSION}" = "3.11" || "${PYTHON_VERSION}" =~ ^3\.11\. ]]; then \
        $HOME/miniconda/bin/conda create -n $CLOUDTIK_ENV -y python=$PYTHON_VERSION; \
    else \
        # Conda issue #12287: ping libffi=3.3 for python version below 3.11
        # TODO: Remove libffi=3.3 once it is resolved
        $HOME/miniconda/bin/conda create -n $CLOUDTIK_ENV -y libffi=3.3 python=$PYTHON_VERSION; \
    fi \
    && $HOME/miniconda/bin/conda clean -itqy
ENV CONDA_ROOT=$HOME/miniconda
ENV PATH=$CONDA_ROOT/envs/$CLOUDTIK_ENV/bin:$PATH
ENV CLOUDTIK_DOWNLOADS="https://d30257nes7d4fq.cloudfront.net/downloads"

WORKDIR $HOME
