#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
LOAD_BALANCER_HOME=$RUNTIME_PATH/loadbalancer

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/loadbalancer/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

configure_loadbalancer() {
    prepare_base_conf
    mkdir -p ${LOAD_BALANCER_HOME}/logs
}

set_head_option "$@"
set_head_address
set_node_address
configure_loadbalancer

exit 0
