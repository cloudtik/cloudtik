#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export HADOOP_VERSION=3.3.1

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

# JDK install function
. "$ROOT_DIR"/common/scripts/jdk-install.sh

# Hadoop install function
. "$ROOT_DIR"/common/scripts/hadoop-install.sh

install_tools() {
    which jq > /dev/null \
      || (sudo apt-get -qq update -y > /dev/null; \
          sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install jq -y > /dev/null)
}

set_head_option "$@"
install_tools
install_jdk
install_hadoop
clean_install
