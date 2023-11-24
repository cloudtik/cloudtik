#!/bin/bash

# Current bin directory
BIN_DIR=`dirname "$0"`
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export MINIO_VERSION=2023-10-16T04-13-43Z

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime
export MINIO_HOME=$RUNTIME_PATH/minio

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

function install_minio() {
    if [ ! -f "${MINIO_HOME}/minio" ]; then
        deb_arch=$(get_deb_arch)
        mkdir -p $RUNTIME_PATH
        (cd $RUNTIME_PATH && wget -q --show-progress https://dl.min.io/server/minio/release/linux-${deb_arch}/archi/minio.RELEASE.${MINIO_VERSION} -O minio.bin && \
          mkdir -p "$MINIO_HOME" && \
          chmod +x minio.bin && \
          mv minio.bin $MINIO_HOME/minio)
        echo "export MINIO_HOME=$MINIO_HOME">> ${USER_HOME}/.bashrc
    fi
}

set_head_option "$@"
install_minio
