#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
REDIS_HOME=$RUNTIME_PATH/redis

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    local source_dir=$(dirname "${BIN_DIR}")/conf
    output_dir=/tmp/redis/conf
    rm -rf  $output_dir
    mkdir -p $output_dir
    cp -r $source_dir/* $output_dir
}

check_redis_installed() {
    if [ ! -d "${REDIS_HOME}" ]; then
        echo "ERROR: Redis is not installed."
        exit 1
    fi
}

update_data_dir() {
    local data_disk_dir=$(get_first_data_disk_dir)
    if [ -z "$data_disk_dir" ]; then
        data_dir="${REDIS_HOME}/data"
    else
        data_dir="$data_disk_dir/redis/data"
    fi

    mkdir -p ${data_dir}
    update_in_file "${config_template_file}" "{%data.dir%}" "${data_dir}"
}

update_server_id() {
    if [ ! -n "${CLOUDTIK_NODE_SEQ_ID}" ]; then
        echo "Replication needs unique server id. No node sequence id allocated for current node!"
        exit 1
    fi

    update_in_file "${config_template_file}" "{%server.id%}" "${CLOUDTIK_NODE_SEQ_ID}"
}

configure_redis() {
    if [ "${IS_HEAD_NODE}" != "true" ] \
        && [ "${REDIS_CLUSTER_MODE}" == "none" ]; then
          return
    fi

    prepare_base_conf

    if [ "${REDIS_CLUSTER_MODE}" == "replication" ]; then
        config_template_file=${output_dir}/redis-replication.conf
    elif [ "${REDIS_CLUSTER_MODE}" == "sharding" ]; then
        config_template_file=${output_dir}/redis-sharding.conf
    else
        config_template_file=${output_dir}/redis.conf
    fi

    mkdir -p ${REDIS_HOME}/logs

    update_in_file "${config_template_file}" "{%bind.ip%}" "${NODE_IP_ADDRESS}"
    update_in_file "${config_template_file}" "{%bind.port%}" "${REDIS_SERVICE_PORT}"
    update_data_dir

    # TODO: WARNING: will the log file get increasingly large
    REDIS_LOG_FILE=${REDIS_HOME}/logs/redis-server.log
    update_in_file "${config_template_file}" "{%log.file%}" "${REDIS_LOG_FILE}"

    if [ "${REDIS_CLUSTER_MODE}" == "sharding" ]; then
        update_in_file "${config_template_file}" "{%cluster.port%}" "${REDIS_CLUSTER_PORT}"
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
        export REDIS_MASTER_HOST=${HEAD_IP_ADDRESS}
    fi

    # check and initialize redis if needed
    bash $BIN_DIR/redis-init.sh >${REDIS_HOME}/logs/redis-init.log 2>&1
}

check_redis_installed
set_head_option "$@"
set_node_address
set_head_address
configure_redis

exit 0
