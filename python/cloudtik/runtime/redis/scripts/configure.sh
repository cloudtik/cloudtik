#!/bin/bash

# Current bin directory
BIN_DIR=`dirname "$0"`
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
REDIS_HOME=$RUNTIME_PATH/redis

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

function prepare_base_conf() {
    local source_dir=$(dirname "${BIN_DIR}")/conf
    output_dir=/tmp/redis/conf
    rm -rf  $output_dir
    mkdir -p $output_dir
    cp -r $source_dir/* $output_dir
}

function check_redis_installed() {
    if [ ! -d "${REDIS_HOME}" ]; then
        echo "ERROR: Redis is not installed."
        exit 1
    fi
}

function update_data_dir() {
    local data_disk_dir=$(get_first_data_disk_dir)
    if [ -z "$data_disk_dir" ]; then
        data_dir="${REDIS_HOME}/data"
    else
        data_dir="$data_disk_dir/redis/data"
    fi

    mkdir -p ${data_dir}
    sed -i "s#{%data.dir%}#${data_dir}#g" ${config_template_file}
}

function update_server_id() {
    if [ ! -n "${CLOUDTIK_NODE_SEQ_ID}" ]; then
        echo "Replication needs unique server id. No node sequence id allocated for current node!"
        exit 1
    fi

    sed -i "s#{%server.id%}#${CLOUDTIK_NODE_SEQ_ID}#g" ${config_template_file}
}

function configure_redis() {
    if [ "${IS_HEAD_NODE}" != "true" ] \
        && [ "${REDIS_CLUSTER_MODE}" == "none" ]; then
          return
    fi

    prepare_base_conf

    if [ "${REDIS_CLUSTER_MODE}" == "replication" ]; then
        config_template_file=${output_dir}/redis-replication.conf
    elif [ "${REDIS_CLUSTER_MODE}" == "cluster" ]; then
        config_template_file=${output_dir}/redis-cluster.conf
    else
        config_template_file=${output_dir}/redis.conf
    fi

    mkdir -p ${REDIS_HOME}/logs

    sed -i "s#{%bind.ip%}#${NODE_IP_ADDRESS}#g" ${config_template_file}
    sed -i "s#{%bind.port%}#${REDIS_SERVICE_PORT}#g" ${config_template_file}
    update_data_dir

    # TODO: WARNING: will the log file get increasingly large
    REDIS_LOG_FILE=${REDIS_HOME}/logs/redis-server.log
    sed -i "s#{%log.file%}#${REDIS_LOG_FILE}#g" ${config_template_file}

    if [ "${REDIS_CLUSTER_MODE}" == "cluster" ]; then
        sed -i "s#{%cluster.port%}#${REDIS_CLUSTER_PORT}#g" ${config_template_file}
    fi

    REDIS_CONFIG_DIR=${REDIS_HOME}/etc
    mkdir -p ${REDIS_CONFIG_DIR}
    REDIS_CONFIG_FILE=${REDIS_CONFIG_DIR}/redis.conf
    cp ${config_template_file} ${REDIS_CONFIG_FILE}

    # This is needed for redis-init.sh to decide whether need to do user db setup
    export REDIS_BASE_DIR=${REDIS_HOME}
    if [ "${IS_HEAD_NODE}" == "true" ]; then
        export REDIS_MASTER_NODE=true
    else
        export REDIS_MASTER_NODE=false
    fi

    if [ "${REDIS_CLUSTER_MODE}" == "replication" ]; then
        export REDIS_MASTER_HOST=${HEAD_ADDRESS}
    fi

    # check and initialize redis if needed
    bash $BIN_DIR/redis-init.sh >${REDIS_HOME}/logs/redis-init.log 2>&1
}

check_redis_installed
set_head_option "$@"
set_node_ip_address
set_head_address
configure_redis

exit 0
