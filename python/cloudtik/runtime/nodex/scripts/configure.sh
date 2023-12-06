#!/bin/bash

# Current bin directory
BIN_DIR=`dirname "$0"`
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
NODEX_HOME=$RUNTIME_PATH/nodex

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

function check_nodex_installed() {
    if [ ! -f "${NODEX_HOME}/nodex" ]; then
        echo "Nodex is not installed."
        exit 1
    fi
}

function configure_nodex() {
    mkdir -p ${NODEX_HOME}/logs
}

set_head_option "$@"
check_nodex_installed
configure_nodex

exit 0
