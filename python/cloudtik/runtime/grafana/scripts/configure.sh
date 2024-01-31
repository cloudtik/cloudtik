#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
GRAFANA_HOME=$RUNTIME_PATH/grafana

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/grafana/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_grafana_installed() {
    if ! command -v grafana &> /dev/null
    then
        echo "Grafana is not installed."
        exit 1
    fi
}

get_service_port() {
    local service_port=3000
    if [ ! -z "${GRAFANA_SERVICE_PORT}" ]; then
        service_port=${GRAFANA_SERVICE_PORT}
    fi
    echo "${service_port}"
}

get_data_dir() {
    data_disk_dir=$(get_first_data_disk_dir)
    if [ -z "$data_disk_dir" ]; then
        data_dir="${GRAFANA_HOME}/data"
    else
        data_dir="$data_disk_dir/grafana/data"
    fi
    echo "${data_dir}"
}

configure_grafana() {
    prepare_base_conf
    CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/grafana.ini

    mkdir -p ${GRAFANA_HOME}/logs
    mkdir -p ${GRAFANA_HOME}/plugins

    GRAFANA_CONFIG_DIR=${GRAFANA_HOME}/conf
    mkdir -p ${GRAFANA_CONFIG_DIR}

    sed -i "s#{%server.address%}#${NODE_IP_ADDRESS}#g" ${CONFIG_TEMPLATE_FILE}

    local SERVER_PORT=$(get_service_port)
    sed -i "s#{%server.port%}#${SERVER_PORT}#g" ${CONFIG_TEMPLATE_FILE}

    local DATA_DIR=$(get_data_dir)
    sed -i "s#{%data.dir%}#${DATA_DIR}#g" ${CONFIG_TEMPLATE_FILE}

    local LOG_DIR=${GRAFANA_HOME}/logs
    sed -i "s#{%logs.dir%}#${LOG_DIR}#g" ${CONFIG_TEMPLATE_FILE}

    local PLUGINS_DIR=${GRAFANA_HOME}/plugins
    sed -i "s#{%plugins.dir%}#${PLUGINS_DIR}#g" ${CONFIG_TEMPLATE_FILE}

    local PROVISIONING_DIR=${GRAFANA_HOME}/conf/provisioning
    sed -i "s#{%provisioning.dir%}#${PROVISIONING_DIR}#g" ${CONFIG_TEMPLATE_FILE}

    cp -r ${CONFIG_TEMPLATE_FILE} ${GRAFANA_CONFIG_DIR}/grafana.ini
}

set_head_option "$@"
check_grafana_installed
set_head_address
set_node_address
configure_grafana

exit 0
