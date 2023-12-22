#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export MONGODB_MAJOR=7.0
export MONGODB_VERSION=7.0.3

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime
export MONGODB_HOME=$RUNTIME_PATH/mongodb

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

install_tools() {
    which curl > /dev/null \
      || (sudo apt-get -qq update -y > /dev/null; \
        sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install curl -y > /dev/null)
}

install_mongodb() {
    if ! command -v mongod &> /dev/null
    then
        local deb_arch=$(get_deb_arch)

        # TODO: remove when curl is installed in the base image
        install_tools

        curl -fsSL https://pgp.mongodb.com/server-7.0.asc \
          | sudo gpg --yes --dearmor -o /usr/share/keyrings/mongodb-server-7.0.gpg
        echo "deb [ arch=${deb_arch} signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg] https://repo.mongodb.org/apt/ubuntu $(lsb_release -cs)/mongodb-org/7.0 multiverse" \
          | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list >/dev/null
        sudo apt-get -qq update -y > /dev/null \
        && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq -y \
          mongodb-org=${MONGODB_VERSION} \
          mongodb-org-database=${MONGODB_VERSION} \
          mongodb-org-server=${MONGODB_VERSION} \
          mongodb-mongosh \
          mongodb-org-mongos \
          mongodb-org-tools > /dev/null
        result=$?
        sudo rm -f /etc/apt/sources.list.d/mongodb-org-7.0.list
        clean_apt
        if [ $result -ne 0 ]; then
            echo "MongoDB installation failed."
            exit 1
        fi
    fi
}

set_head_option "$@"
install_mongodb
