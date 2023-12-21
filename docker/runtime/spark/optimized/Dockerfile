ARG BASE_TAG="nightly"
FROM cloudtik/cloudtik:$BASE_TAG

ARG SPARK_VERSION=3.3.0

ENV RUNTIME_PATH /home/cloudtik/runtime
RUN mkdir -p $RUNTIME_PATH
WORKDIR /home/cloudtik/runtime

SHELL ["/bin/bash", "-c"]
RUN IFS=', ' read -r -a runtime_names <<< "nodex, hadoop, mount, yarn, spark" \
    && export PATH="$CONDA_ROOT/envs/$CLOUDTIK_ENV/bin:$PATH" \
    && export CLOUDTIK_INSTALL_ALL=true \
    && export SPARK_DOWNLOAD_URL=${CLOUDTIK_DOWNLOADS}/spark/spark-${SPARK_VERSION}-bin-hadoop3.tgz
    && (for runtime_name in ${runtime_names[@]}; do \
            echo "Installing ${runtime_name}"; \
            cloudtik runtime install ${runtime_name} --head; \
        done; \
        ) \
    && pip uninstall cloudtik -y

WORKDIR /home/cloudtik/
