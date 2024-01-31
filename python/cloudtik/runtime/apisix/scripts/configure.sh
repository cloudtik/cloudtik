#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
APISIX_HOME=$RUNTIME_PATH/apisix

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/apisix/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_apisix_installed() {
    if ! command -v apisix &> /dev/null
    then
        echo "APISIX is not installed."
        exit 1
    fi
}

configure_apisix() {
    prepare_base_conf
    mkdir -p ${APISIX_HOME}/logs

    APISIX_CONF_DIR=${APISIX_HOME}/conf
    mkdir -p ${APISIX_CONF_DIR}

    CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/config.yaml

    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%listen.ip%}" "${NODE_IP_ADDRESS}"
    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%listen.port%}" "${APISIX_SERVICE_PORT}"
    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%admin.key%}" "${APISIX_ADMIN_KEY}"
    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%admin.port%}" "${APISIX_ADMIN_PORT}"
    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%cluster.name%}" "${CLOUDTIK_CLUSTER}"

    cp ${CONFIG_TEMPLATE_FILE} ${APISIX_CONF_DIR}/config.yaml
}

set_head_option "$@"
check_apisix_installed
set_node_address
configure_apisix

exit 0
