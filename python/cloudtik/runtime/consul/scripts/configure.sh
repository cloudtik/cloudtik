#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
CONSUL_HOME=$RUNTIME_PATH/consul

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/consul/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_consul_installed() {
    if ! command -v consul &> /dev/null
    then
        echo "Consul is not installed."
        exit 1
    fi
}

update_consul_data_dir() {
    data_disk_dir=$(get_first_data_disk_dir)
    if [ -z "$data_disk_dir" ]; then
        consul_data_dir="$CONSUL_HOME/data"
    else
        consul_data_dir="$data_disk_dir/consul/data"
    fi

    mkdir -p ${consul_data_dir}
    update_in_file ${CONFIG_TEMPLATE_FILE} "{%data.dir%}" "${consul_data_dir}"
}

update_ui_config() {
    if [ "$IS_HEAD_NODE" == "true" ]; then
        UI_ENABLED=true
    else
        UI_ENABLED=false
    fi
    update_in_file ${CONFIG_SERVER_TEMPLATE_FILE} "{%ui.enabled%}" "${UI_ENABLED}"
}

configure_consul() {
    prepare_base_conf
    CONSUL_OUTPUT_DIR=${OUTPUT_DIR}/consul
    CONFIG_TEMPLATE_FILE=${CONSUL_OUTPUT_DIR}/consul.json
    CONFIG_SERVER_TEMPLATE_FILE=${CONSUL_OUTPUT_DIR}/server.json

    mkdir -p ${CONSUL_HOME}/logs

    # General agent configuration. retry_join will be set in python script
    local DATA_CENTER=default
    if [ ! -z "${CONSUL_DATA_CENTER}" ]; then
        DATA_CENTER=${CONSUL_DATA_CENTER}
    fi
    update_in_file ${CONFIG_TEMPLATE_FILE} "{%data.center%}" "${DATA_CENTER}"
    update_in_file ${CONFIG_TEMPLATE_FILE} "{%bind.address%}" "${NODE_IP_ADDRESS}"

    if [ "${CONSUL_SERVER}" == "true" ]; then
        # client address bind to both node ip and local host
        CLIENT_ADDRESS="${NODE_IP_ADDRESS} 127.0.0.1"
    else
        # bind to local host for client
        CLIENT_ADDRESS="127.0.0.1"
    fi
    update_in_file ${CONFIG_TEMPLATE_FILE} "{%client.address%}" "${CLIENT_ADDRESS}"
    update_in_file ${CONFIG_TEMPLATE_FILE} "{%rpc.port%}" "${CONSUL_SERVICE_PORT}"
    update_in_file ${CONFIG_TEMPLATE_FILE} "{%client.port%}" "${CONSUL_CLIENT_PORT}"
    update_in_file ${CONFIG_TEMPLATE_FILE} "{%dns.port%}" "${CONSUL_DNS_PORT}"

    update_consul_data_dir

    if [ "${CONSUL_SERVER}" == "true" ]; then
        # Server agent configuration
        update_in_file ${CONFIG_SERVER_TEMPLATE_FILE} "{%number.servers%}" "${CONSUL_NUM_SERVERS}"
        update_ui_config
    fi

    CONSUL_CONFIG_DIR=${CONSUL_HOME}/consul.d
    mkdir -p ${CONSUL_CONFIG_DIR}
    cp ${CONFIG_TEMPLATE_FILE} ${CONSUL_CONFIG_DIR}/consul.json
    chmod 640 ${CONSUL_CONFIG_DIR}/consul.json

    if [ "${CONSUL_SERVER}" == "true" ]; then
        cp ${CONFIG_SERVER_TEMPLATE_FILE} ${CONSUL_CONFIG_DIR}/server.json
        chmod 640 ${CONSUL_CONFIG_DIR}/server.json
    fi
}

set_head_option "$@"
check_consul_installed
set_head_address
set_node_address
configure_consul

exit 0
