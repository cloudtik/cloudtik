ARG BASE_IMAGE="nightly"
FROM cloudtik/cloudtik-base:"$BASE_IMAGE"
ARG WHEEL_PATH
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

COPY $WHEEL_PATH .
RUN export PATH="$CONDA_ROOT/envs/$CLOUDTIK_ENV/bin:$PATH" \
    && pip --no-cache-dir install $(basename $WHEEL_PATH)[all] \
    && pip uninstall cloudtik -y \
    && sudo rm $(basename $WHEEL_PATH)
