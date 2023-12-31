#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export ZOOKEEPER_VERSION=3.7.2

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime

# JDK install function
. "$ROOT_DIR"/common/scripts/jdk-install.sh

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

install_zookeeper() {
    # install zookeeper
    export ZOOKEEPER_HOME=$RUNTIME_PATH/zookeeper

    if [ ! -d "${ZOOKEEPER_HOME}" ]; then
        mkdir -p $RUNTIME_PATH
        (cd $RUNTIME_PATH \
          && wget -q --show-progress \
            https://downloads.apache.org/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz -O zookeeper.tar.gz \
          && mkdir -p "$ZOOKEEPER_HOME" \
          && tar --extract --file zookeeper.tar.gz --directory "$ZOOKEEPER_HOME" --strip-components 1 --no-same-owner \
          && rm -f zookeeper.tar.gz)
        if [ $? -ne 0 ]; then
            echo "Zookeeper installation failed."
            exit 1
        fi
        echo "export ZOOKEEPER_HOME=$ZOOKEEPER_HOME">> ${USER_HOME}/.bashrc
        echo "export PATH=\$ZOOKEEPER_HOME/bin:\$PATH" >> ${USER_HOME}/.bashrc
    fi
}

set_head_option "$@"
install_jdk
install_zookeeper
clean_install
