ARG BASE_TAG="nightly"
FROM cloudtik/spark-runtime-benchmark:$BASE_TAG

RUN sudo sed -i 's/archive.ubuntu.com/mirrors.aliyun.com/' /etc/apt/sources.list \
    && sudo sed -i 's/security.ubuntu.com/mirrors.aliyun.com/' /etc/apt/sources.list \
    && pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple/
