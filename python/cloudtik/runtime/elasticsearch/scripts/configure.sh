#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
ELASTICSEARCH_HOME=$RUNTIME_PATH/elasticsearch

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

# Load elasticsearch functions
. "$BIN_DIR"/elasticsearch.sh

prepare_base_conf() {
    local source_dir=$(dirname "${BIN_DIR}")/conf
    output_dir=/tmp/elasticsearch/conf
    rm -rf  $output_dir
    mkdir -p $output_dir
    cp -r $source_dir/* $output_dir
}

check_elasticsearch_installed() {
    if [ ! -d "${ELASTICSEARCH_HOME}" ]; then
        echo "ElasticSearch is not installed."
        exit 1
    fi
}

update_data_dir() {
    local data_dir=$(get_data_disk_dirs_of "elasticsearch/data" true)
    # if no disks mounted
    if [ -z "$data_dir" ]; then
        data_dir="${ELASTICSEARCH_HOME}/data"
        mkdir -p $data_dir
    fi

    update_in_file "${config_template_file}" \
      "{%path.data%}" "${data_dir}"
}

update_node_name() {
    if [ ! -n "${CLOUDTIK_NODE_SEQ_ID}" ]; then
        echo "No node sequence id allocated for current node."
        exit 1
    fi
    local -r node_name="node-${CLOUDTIK_NODE_SEQ_ID}"
    update_in_file "${config_template_file}" \
      "{%node.name%}" "${node_name}"
}

configure_variable() {
    set_variable_in_file "${ELASTICSEARCH_CONFIG_DIR}/elasticsearch" "$@"
}

configure_service_init() {
    echo "# ElasticSearch init variables" > ${ELASTICSEARCH_CONFIG_DIR}/elasticsearch
}

configure_elasticsearch() {
    if [ "${IS_HEAD_NODE}" != "true" ] \
        && [ "${ELASTICSEARCH_CLUSTER_MODE}" == "none" ]; then
          return
    fi

    prepare_base_conf

    if [ "${ELASTICSEARCH_CLUSTER_MODE}" == "cluster" ]; then
        config_template_file=${output_dir}/elasticsearch-cluster.yml
    else
        config_template_file=${output_dir}/elasticsearch.yml
    fi

    ELASTICSEARCH_LOG_DIR="${ELASTICSEARCH_HOME}/logs"
    mkdir -p ${ELASTICSEARCH_LOG_DIR}

    update_in_file "${config_template_file}" \
      "{%cluster.name%}" "${CLOUDTIK_CLUSTER}"
    update_node_name

    update_in_file "${config_template_file}" \
      "{%bind.ip%}" "${NODE_IP_ADDRESS}"
    update_in_file "${config_template_file}" \
      "{%node.host%}" "${NODE_HOST_ADDRESS}"
    update_in_file "${config_template_file}" \
      "{%bind.port%}" "${ELASTICSEARCH_SERVICE_PORT}"
    update_in_file "${config_template_file}" \
      "{%transport.port%}" "${ELASTICSEARCH_TRANSPORT_PORT}"

    update_data_dir
    update_in_file "${config_template_file}" \
      "{%path.logs%}" "${ELASTICSEARCH_LOG_DIR}"

    update_in_file "${config_template_file}" \
      "{%security.enabled%}" "${ELASTICSEARCH_SECURITY}"

    if [ ! -z "$ELASTICSEARCH_PASSWORD" ]; then
        elasticsearch_set_key_value "bootstrap.password" "$ELASTICSEARCH_PASSWORD"
    fi

    ELASTICSEARCH_CONFIG_DIR=${ELASTICSEARCH_HOME}/config
    mkdir -p ${ELASTICSEARCH_CONFIG_DIR}
    ELASTICSEARCH_CONFIG_FILE=${ELASTICSEARCH_CONFIG_DIR}/elasticsearch.yml
    cp ${config_template_file} ${ELASTICSEARCH_CONFIG_FILE}

    # Set variables for export to elasticsearch-init.sh
    configure_service_init
}

check_elasticsearch_installed
set_head_option "$@"
set_node_address
set_head_address
configure_elasticsearch

exit 0
