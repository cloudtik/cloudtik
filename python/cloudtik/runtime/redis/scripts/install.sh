#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export REDIS_VERSION=7.2.3

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime
export REDIS_HOME=$RUNTIME_PATH/redis

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

install_redis() {
    if [ ! -d "${REDIS_HOME}" ]; then
        mkdir -p $RUNTIME_PATH

        # curl -fsSL https://packages.redis.io/gpg \
        #   | sudo gpg --yes --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
        # echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" \
        #   | sudo tee /etc/apt/sources.list.d/redis.list >/dev/null
        # sudo apt-get -qq update -y > /dev/null && \
        # sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq -y \
        #   redis=${REDIS_VERSION} > /dev/null && \
        # sudo rm -f /etc/apt/sources.list.d/redis.list

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
