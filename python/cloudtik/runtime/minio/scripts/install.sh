#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export MINIO_VERSION=2023-11-20T22-40-07Z
export MINIO_CLIENT_VERSION=2023-11-20T16-30-59Z

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime
export MINIO_HOME=$RUNTIME_PATH/minio

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

install_minio() {
    if [ ! -f "${MINIO_HOME}/bin/minio" ]; then
        deb_arch=$(get_deb_arch)
        mkdir -p $RUNTIME_PATH
        (cd $RUNTIME_PATH \
          && wget -q --show-progress \
            https://dl.min.io/server/minio/release/linux-${deb_arch}/archive/minio.RELEASE.${MINIO_VERSION} -O minio.bin \
          && mkdir -p "$MINIO_HOME/bin" \
          && chmod +x minio.bin \
          && mv minio.bin $MINIO_HOME/bin/minio)
        if [ $? -ne 0 ]; then
            echo "Minio installation failed."
            exit 1
        fi
        echo "export MINIO_HOME=$MINIO_HOME">> ${USER_HOME}/.bashrc

        if [ "$IS_HEAD_NODE" == "true" ]; then
            # Download mc cli on head
            (cd $RUNTIME_PATH \
              && wget -q --show-progress \
                https://dl.min.io/client/mc/release/linux-${deb_arch}/archive/mc.RELEASE.${MINIO_CLIENT_VERSION} -O mc.bin \
              && chmod +x mc.bin \
              && mv mc.bin $MINIO_HOME/bin/mc)
            echo "export PATH=\$MINIO_HOME/bin:\$PATH" >> ${USER_HOME}/.bashrc
        fi
    fi
}

set_head_option "$@"
install_minio
