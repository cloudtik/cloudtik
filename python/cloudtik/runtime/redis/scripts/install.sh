#!/bin/bash

# Current bin directory
BIN_DIR=`dirname "$0"`
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export REDIS_VERSION=7.2.3

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime
export REDIS_HOME=$RUNTIME_PATH/redis

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

function install_redis() {
    if [ ! -d "${REDIS_HOME}" ]; then
        mkdir -p $RUNTIME_PATH
        local deb_arch=$(get_deb_arch)
        # Currently we download from bitnami, we can build in the future
        REDIS_COMPONENT="redis-${REDIS_VERSION}-1-linux-${deb_arch}-debian-11"
        (cd $RUNTIME_PATH && \
          wget -q --show-progress \
            "https://downloads.bitnami.com/files/stacksmith/${REDIS_COMPONENT}.tar.gz" -O ${REDIS_COMPONENT}.tar.gz && \
          wget -q \
            "https://downloads.bitnami.com/files/stacksmith/${REDIS_COMPONENT}.tar.gz.sha256" -O ${REDIS_COMPONENT}.tar.gz.sha256 && \
          sha256sum -c "${REDIS_COMPONENT}.tar.gz.sha256" && \
          mkdir -p "$REDIS_HOME" && \
          tar --extract --file "${REDIS_COMPONENT}".tar.gz --directory "$REDIS_HOME" --strip-components 3 --no-same-owner && \
          rm -rf "${REDIS_COMPONENT}".tar.gz{,.sha256} && \
          echo "export REDIS_HOME=$REDIS_HOME" >> ${USER_HOME}/.bashrc && \
          echo "export PATH=\$REDIS_HOME/bin:\$PATH" >> ${USER_HOME}/.bashrc)
    fi
}

set_head_option "$@"
install_redis
