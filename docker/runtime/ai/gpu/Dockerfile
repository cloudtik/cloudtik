ARG BASE_PREFIX="cloudtik"
ARG BASE_TAG="nightly"
FROM cloudtik/${BASE_PREFIX}:$BASE_TAG
ARG WHEEL_PATH

ENV RUNTIME_PATH /home/cloudtik/runtime
RUN mkdir -p $RUNTIME_PATH
WORKDIR /home/cloudtik/runtime

ENV AI_WITH_GPU true

COPY $WHEEL_PATH .
SHELL ["/bin/bash", "-c"]
RUN export PATH="$CONDA_ROOT/envs/$CLOUDTIK_ENV/bin:$PATH" \
    && pip --no-cache-dir install `basename $WHEEL_PATH`[all] \
    && sudo rm `basename $WHEEL_PATH` \
    && export CLOUDTIK_INSTALL_ALL=true \
    && IFS=', ' read -r -a runtime_names <<< "ai" \
    && (for runtime_name in ${runtime_names[@]}; do \
            echo "Installing ${runtime_name}"; \
            cloudtik runtime install ${runtime_name} --head; \
        done; \
        ) \
    && pip uninstall cloudtik -y

WORKDIR /home/cloudtik/
