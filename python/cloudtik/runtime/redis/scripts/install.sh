#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export REDIS_MAJOR_VERSION="7.2.*"

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime
export REDIS_HOME=$RUNTIME_PATH/redis

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

install_redis() {
    if [ ! -d "${REDIS_HOME}" ]; then
        curl -fsSL https://packages.redis.io/gpg \
          | sudo gpg --yes --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg \
          && echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" \
            | sudo tee /etc/apt/sources.list.d/redis.list >/dev/null \
          && sudo apt-get -qq update -y > /dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq -y \
            redis=6:${REDIS_MAJOR_VERSION} > /dev/null
        result=$?
        sudo rm -f /etc/apt/sources.list.d/redis.list
        if [ $result -ne 0 ]; then
            echo "Redis installation failed."
            exit 1
        fi
        mkdir -p ${REDIS_HOME}
        echo "export REDIS_HOME=$REDIS_HOME" >> ${USER_HOME}/.bashrc
        echo "export PATH=\$REDIS_HOME/bin:\$PATH" >> ${USER_HOME}/.bashrc
    fi
}

set_head_option "$@"
install_redis
