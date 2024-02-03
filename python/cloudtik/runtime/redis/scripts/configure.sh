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
    OUTPUT_DIR=/tmp/redis/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_redis_installed() {
    if ! command -v redis-server &> /dev/null
    then
        echo "Redis is not installed."
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
    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%data.dir%}" "${data_dir}"
}

update_node_name() {
    if [ ! -n "${CLOUDTIK_NODE_SEQ_ID}" ]; then
        echo "No node sequence id allocated for current node."
        exit 1
    fi
    local -r node_name="${CLOUDTIK_CLUSTER}-${CLOUDTIK_NODE_SEQ_ID}"
    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%cluster.nodename%}" "${node_name}"
}

configure_variable() {
    set_variable_in_file "${REDIS_CONFIG_DIR}/redis" "$@"
}

configure_service_init() {
    echo "# Redis init variables" > ${REDIS_CONFIG_DIR}/redis

    configure_variable REDIS_CONF_FILE "${REDIS_CONFIG_FILE}"
    configure_variable REDIS_BASE_DIR "${REDIS_HOME}"
    configure_variable REDIS_PORT "${REDIS_SERVICE_PORT}"
    configure_variable REDIS_HEAD_NODE ${IS_HEAD_NODE}
    configure_variable REDIS_HEAD_HOST "${HEAD_HOST_ADDRESS}"
    configure_variable REDIS_NODE_IP "${NODE_IP_ADDRESS}"
    configure_variable REDIS_NODE_HOST "${NODE_HOST_ADDRESS}"
    configure_variable REDIS_CLUSTER_MODE "${REDIS_CLUSTER_MODE}"

    if [ "${REDIS_CLUSTER_MODE}" == "replication" ]; then
        # The default primary host
        configure_variable REDIS_PRIMARY_HOST "${HEAD_HOST_ADDRESS}"
        # The default role assigned if there is not sentinel
        local role="primary"
        if [ "${IS_HEAD_NODE}" != "true" ]; then
            role="secondary"
        fi
        configure_variable REDIS_REPLICATION_ROLE "$role"

        if [ "${REDIS_SENTINEL_ENABLED}" == "true" ]; then
            configure_variable REDIS_SENTINEL_MASTER_NAME "${CLOUDTIK_CLUSTER}"
            configure_variable REDIS_SENTINEL_DATA_DIR "${REDIS_SENTINEL_DATA_DIR}"
            configure_variable REDIS_SENTINEL_CONF_FILE "${REDIS_SENTINEL_DATA_DIR}/redis-sentinel.conf"
        fi
    fi

    # TODO: further improve the security of the password in file
    configure_variable REDIS_PASSWORD "${REDIS_PASSWORD}"

    # make it owner only read/write for security
    chmod 0600 "${REDIS_CONFIG_DIR}/redis"
}

get_sentinel_data_dir() {
    local data_disk_dir=$(get_first_data_disk_dir)
    local sentinel_data_dir
    if [ -z "$data_disk_dir" ]; then
        sentinel_data_dir="${REDIS_HOME}/sentinel"
    else
        sentinel_data_dir="$data_disk_dir/redis/sentinel"
    fi
    echo "${sentinel_data_dir}"
}

configure_sentinel() {
    SENTINEL_TEMPLATE_FILE=${OUTPUT_DIR}/redis-sentinel.conf

    local -r sentinel_data_dir=$(get_sentinel_data_dir)
    REDIS_SENTINEL_DATA_DIR="${sentinel_data_dir}"

    local -r sentinel_init_file=${sentinel_data_dir}/.initialized
    if [ ! -f "${sentinel_init_file}" ]; then
        # configure only once because the configure is part of data of sentinel
        mkdir -p ${sentinel_data_dir}
        update_in_file "${SENTINEL_TEMPLATE_FILE}" \
          "{%sentinel.data.dir%}" "${sentinel_data_dir}"
        update_in_file "${SENTINEL_TEMPLATE_FILE}" \
          "{%sentinel.bind.ip%}" "${NODE_IP_ADDRESS}"
        update_in_file "${SENTINEL_TEMPLATE_FILE}" \
          "{%sentinel.port%}" "${REDIS_SENTINEL_PORT}"
        update_in_file "${SENTINEL_TEMPLATE_FILE}" \
          "{%sentinel.pidfile%}" "${REDIS_HOME}/redis-sentinel.pid"
        update_in_file "${SENTINEL_TEMPLATE_FILE}" \
          "{%sentinel.log.file%}" "${REDIS_HOME}/logs/redis-sentinel.log"
        update_in_file "${SENTINEL_TEMPLATE_FILE}" \
          "{%sentinel.master.name%}" "${CLOUDTIK_CLUSTER}"
        cp ${SENTINEL_TEMPLATE_FILE} "${sentinel_data_dir}/redis-sentinel.conf"
    fi
}

configure_redis() {
    if [ "${IS_HEAD_NODE}" != "true" ] \
        && [ "${REDIS_CLUSTER_MODE}" == "none" ]; then
          return
    fi

    prepare_base_conf

    if [ "${REDIS_CLUSTER_MODE}" == "replication" ]; then
        CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/redis-replication.conf
    elif [ "${REDIS_CLUSTER_MODE}" == "sharding" ]; then
        CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/redis-sharding.conf
    else
        CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/redis.conf
    fi

    mkdir -p ${REDIS_HOME}/logs

    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%bind.ip%}" "${NODE_IP_ADDRESS}"
    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%bind.port%}" "${REDIS_SERVICE_PORT}"
    update_data_dir

    # TODO: WARNING: will the log file get increasingly large
    REDIS_LOG_FILE=${REDIS_HOME}/logs/redis-server.log
    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%log.file%}" "${REDIS_LOG_FILE}"

    if [ "${REDIS_CLUSTER_MODE}" == "sharding" ]; then
        update_in_file "${CONFIG_TEMPLATE_FILE}" "{%cluster.port%}" "${REDIS_CLUSTER_PORT}"
        update_node_name
    fi

    REDIS_CONFIG_DIR=${REDIS_HOME}/etc
    mkdir -p ${REDIS_CONFIG_DIR}
    REDIS_CONFIG_FILE=${REDIS_CONFIG_DIR}/redis.conf
    cp ${CONFIG_TEMPLATE_FILE} ${REDIS_CONFIG_FILE}

    # sentinel
    if [ "${REDIS_SENTINEL_ENABLED}" == "true" ]; then
        configure_sentinel
    fi

    # Set variables for export to redis-init.sh
    configure_service_init
}

check_redis_installed
set_head_option "$@"
set_node_address
set_head_address
configure_redis

exit 0
