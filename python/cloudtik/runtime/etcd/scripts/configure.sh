#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
ETCD_HOME=$RUNTIME_PATH/etcd

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/etcd/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_etcd_installed() {
    if ! command -v etcd &> /dev/null
    then
        echo "ETCD is not installed."
        exit 1
    fi
}

update_data_dir() {
    data_disk_dir=$(get_first_data_disk_dir)
    if [ -z "$data_disk_dir" ]; then
        data_dir="${ETCD_HOME}/data"
    else
        data_dir="$data_disk_dir/etcd/data"
    fi

    mkdir -p ${data_dir}
    sed -i "s#{%data.dir%}#${data_dir}#g" ${CONFIG_TEMPLATE_FILE}
}

configure_etcd() {
    prepare_base_conf

    ETC_LOG_DIR=${ETCD_HOME}/logs
    mkdir -p ${ETC_LOG_DIR}

    CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/etcd.yaml
    sed -i "s#{%node.ip%}#${NODE_IP_ADDRESS}#g" ${CONFIG_TEMPLATE_FILE}
    sed -i "s#{%node.host%}#${NODE_HOST_ADDRESS}#g" ${CONFIG_TEMPLATE_FILE}

    NODE_NAME="server${CLOUDTIK_NODE_SEQ_ID}"
    sed -i "s#{%node.name%}#${NODE_NAME}#g" ${CONFIG_TEMPLATE_FILE}

    update_data_dir

    ETC_LOG_FILE=${ETC_LOG_DIR}/etcd-server.log
    sed -i "s#{%log.file%}#${ETC_LOG_FILE}#g" ${CONFIG_TEMPLATE_FILE}

    sed -i "s#{%initial.cluster.token%}#${ETCD_CLUSTER_NAME}#g" ${CONFIG_TEMPLATE_FILE}

    if [ "${CLOUDTIK_NODE_QUORUM_JOIN}" == "init" ]; then
        INITIAL_CLUSTER_STATE=existing
    else
        INITIAL_CLUSTER_STATE=new
    fi
    sed -i "s#{%initial.cluster.state%}#${INITIAL_CLUSTER_STATE}#g" ${CONFIG_TEMPLATE_FILE}

    ETCD_CONFIG_DIR=${ETCD_HOME}/conf
    mkdir -p ${ETCD_CONFIG_DIR}
    cp -r ${CONFIG_TEMPLATE_FILE} ${ETCD_CONFIG_DIR}/etcd.yaml
}

set_head_option "$@"

if [ "${IS_HEAD_NODE}" == "false" ]; then
    check_etcd_installed
    set_head_address
    set_node_address
    configure_etcd
fi

exit 0
