#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
PROMETHEUS_HOME=$RUNTIME_PATH/prometheus

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/prometheus/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_prometheus_installed() {
    if [ ! -f "${PROMETHEUS_HOME}/prometheus" ]; then
        echo "Prometheus is not installed."
        exit 1
    fi
}

update_local_file() {
    cp -r ${OUTPUT_DIR}/scrape-config-local-file.yaml \
      ${PROMETHEUS_CONFIG_DIR}/scrape-config-local-file.yaml
}

update_local_consul() {
  cp -r ${OUTPUT_DIR}/scrape-config-local-consul.yaml \
      ${PROMETHEUS_CONFIG_DIR}/scrape-config-local-consul.yaml
}

update_workspace_consul() {
  cp -r ${OUTPUT_DIR}/scrape-config-workspace-consul.yaml \
      ${PROMETHEUS_CONFIG_DIR}/scrape-config-workspace-consul.yaml
}

update_federation_consul() {
  # Federation will also scrape local cluster
  update_local_consul
  cp -r ${OUTPUT_DIR}/scrape-config-federation-consul.yaml \
      ${PROMETHEUS_CONFIG_DIR}/scrape-config-federation-consul.yaml
}

update_federation_file() {
  # Federation will also scrape local cluster
  update_local_file
  cp -r ${OUTPUT_DIR}/scrape-config-federation-file.yaml \
      ${PROMETHEUS_CONFIG_DIR}/scrape-config-federation-file.yaml
}

configure_prometheus() {
    prepare_base_conf
    CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/prometheus.yaml

    mkdir -p ${PROMETHEUS_HOME}/logs

    PROMETHEUS_CONFIG_DIR=${PROMETHEUS_HOME}/conf
    mkdir -p ${PROMETHEUS_CONFIG_DIR}

    sed -i "s#{%prometheus.home%}#${PROMETHEUS_HOME}#g" `grep "{%prometheus.home%}" -rl ${OUTPUT_DIR}`
    sed -i "s#{%workspace.name%}#${CLOUDTIK_WORKSPACE}#g" `grep "{%workspace.name%}" -rl ${OUTPUT_DIR}`
    sed -i "s#{%cluster.name%}#${CLOUDTIK_CLUSTER}#g" `grep "{%cluster.name%}" -rl ${OUTPUT_DIR}`

    if [ "${PROMETHEUS_SCRAPE_SCOPE}" == "workspace" ]; then
        if [ "${PROMETHEUS_SERVICE_DISCOVERY}" == "consul" ]; then
            update_workspace_consul
        fi
    elif [ "${PROMETHEUS_SCRAPE_SCOPE}" == "federation" ]; then
        if [ "${PROMETHEUS_SERVICE_DISCOVERY}" == "consul" ]; then
            update_federation_consul
        elif [ "${PROMETHEUS_SERVICE_DISCOVERY}" == "file" ]; then
            update_federation_file
        fi
    else
        # local scope
        if [ "${PROMETHEUS_SERVICE_DISCOVERY}" == "consul" ]; then
            update_local_consul
        elif [ "${PROMETHEUS_SERVICE_DISCOVERY}" == "file" ]; then
            update_local_file
        fi
    fi

    cp -r ${CONFIG_TEMPLATE_FILE} ${PROMETHEUS_CONFIG_DIR}/prometheus.yaml
}

set_head_option "$@"
check_prometheus_installed
set_head_address
set_node_address
configure_prometheus

exit 0
