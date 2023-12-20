ARG RUNTIME_IMAGE="universe"
ARG BASE_TAG="nightly"
FROM cloudtik/$RUNTIME_IMAGE:$BASE_TAG

RUN sudo sed -i 's/archive.ubuntu.com/mirrors.aliyun.com/' /etc/apt/sources.list \
    && sudo sed -i 's/security.ubuntu.com/mirrors.aliyun.com/' /etc/apt/sources.list \
    && pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple/
