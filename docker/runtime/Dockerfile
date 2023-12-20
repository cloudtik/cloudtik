ARG RUNTIME_IMAGE=""
ARG BASE_TAG="nightly"
FROM cloudtik/cloudtik:$BASE_TAG

ARG IMAGE_RUNTIMES=""

ENV RUNTIME_PATH /home/cloudtik/runtime
RUN mkdir -p $RUNTIME_PATH
WORKDIR /home/cloudtik/runtime

SHELL ["/bin/bash", "-c"]
RUN IFS=', ' read -r -a runtime_names <<< "$IMAGE_RUNTIMES" \
      && export PATH="$CONDA_ROOT/envs/$CLOUDTIK_ENV/bin:$PATH" \
      && export CLOUDTIK_INSTALL_ALL=true \
      && (for runtime_name in ${runtime_names[@]}; do \
            echo "Installing ${runtime_name}"; \
            cloudtik runtime install ${runtime_name} --head; \
          done; \
          )

WORKDIR /home/cloudtik/
