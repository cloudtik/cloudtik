#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
XINETD_HOME=$RUNTIME_PATH/xinetd

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

check_xinetd_installed() {
    if ! command -v xinetd &> /dev/null
    then
        echo "xinetd is not installed."
        exit 1
    fi
}

prepare_base_conf() {
    source_dir=$(dirname "${BIN_DIR}")/conf
    output_dir=/tmp/xinetd/conf
    rm -rf  $output_dir
    mkdir -p $output_dir
    cp -r $source_dir/* $output_dir
}

configure_xinetd() {
    prepare_base_conf

    mkdir -p ${XINETD_HOME}/logs
    XINETD_CONFIG_DIR=${XINETD_HOME}/conf
    mkdir -p ${XINETD_CONFIG_DIR}/xinetd.d

    ETC_DEFAULT=/etc/default
    sudo mkdir -p ${ETC_DEFAULT}

    xinetd_file=${output_dir}/xinetd
    update_in_file "${xinetd_file}" "{%xinetd.home%}" "${XINETD_HOME}"
    sudo cp ${xinetd_file} ${ETC_DEFAULT}/xinetd

    xinetd_config_file=${output_dir}/xinetd.conf
    update_in_file "${xinetd_config_file}" "{%xinetd.home%}" "${XINETD_HOME}"

    cp ${xinetd_config_file} ${XINETD_CONFIG_DIR}/xinetd.conf

    # generate the service definitions in the xinetd.d
}

set_head_option "$@"
check_xinetd_installed
configure_xinetd

exit 0
