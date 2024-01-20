#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime
export XINETD_HOME=$RUNTIME_PATH/xinetd

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

install_xinetd() {
    if ! command -v xinetd &> /dev/null
    then
        sudo apt-get -qq update -y > /dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install -y xinetd > /dev/null
        if [ $? -ne 0 ]; then
            echo "xinetd installation failed."
            exit 1
        fi
        mkdir -p ${XINETD_HOME}
    fi
}

set_head_option "$@"
install_xinetd
clean_install
