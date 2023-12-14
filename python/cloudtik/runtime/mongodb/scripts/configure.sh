#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
MONGODB_HOME=$RUNTIME_PATH/mongodb

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    local source_dir=$(dirname "${BIN_DIR}")/conf
    output_dir=/tmp/mongodb/conf
    rm -rf  $output_dir
    mkdir -p $output_dir
    cp -r $source_dir/* $output_dir
}

check_mongodb_installed() {
    if ! command -v mongod &> /dev/null
    then
        echo "MongoDB is not installed for mongod command is not available."
        exit 1
    fi
}

update_data_dir() {
    local data_disk_dir=$(get_first_data_disk_dir)
    if [ -z "$data_disk_dir" ]; then
        VOLUME_DIR="${MONGODB_HOME}"
    else
        VOLUME_DIR="$data_disk_dir/mongodb"
    fi

    DATA_DIR="${VOLUME_DIR}/data"
    mkdir -p ${DATA_DIR}
    update_in_file "${config_template_file}" "{%data.dir%}" "${DATA_DIR}"
}

turn_on_start_replication_on_boot() {
    if [ "${IS_HEAD_NODE}" != "true" ]; then
        # only do this for workers for now, head needs handle differently for sharding
        if [ "${MONGODB_CLUSTER_MODE}" == "replication" ]; then
            update_in_file "${MONGODB_CONFIG_FILE}" "^skip_replica_start=ON" "skip_replica_start=OFF"
        elif [ "${MONGODB_CLUSTER_MODE}" == "sharding" ]; then
            update_in_file "${MONGODB_CONFIG_FILE}" "^sharding_start_on_boot=OFF" "sharding_start_on_boot=ON"
        fi
    fi
}

configure_common() {
    local -r config_file="${1:-${config_template_file}}"
      # TODO: can bind to a hostname instead of IP if hostname is stable
    update_in_file "${config_file}" "{%bind.address%}" "${NODE_IP_ADDRESS}"
    update_in_file "${config_file}" "{%home.dir%}" "${MONGODB_HOME}"
}

configure_mongod() {
    configure_common
    update_in_file "${config_template_file}" "{%bind.port%}" "${MONGODB_SERVICE_PORT}"
    update_data_dir
}

configure_mongos() {
    local -r config_file="${1:-${config_template_file}}"
    configure_common "${config_file}"
    update_in_file "${config_file}" "{%bind.port%}" "${MONGODB_MONGOS_SERVICE_PORT}"
}

configure_replica_set() {
    update_in_file "${config_template_file}" \
          "{%replication.set.name%}" "${MONGODB_REPLICATION_SET_NAME}"
}

set_env_for_init() {
    export MONGODB_BIN_DIR="$( dirname -- "$(which mongod)" )"
    export MONGODB_CONF_DIR="${MONGODB_CONFIG_DIR}"
    export MONGODB_CONF_FILE="${MONGODB_CONFIG_FILE}"
    export MONGODB_DATA_DIR="${DATA_DIR}"
    export MONGODB_PID_FILE="${MONGODB_HOME}/mongod.pid"
    export MONGODB_VOLUME_DIR="${VOLUME_DIR}"
    export MONGODB_PORT=${MONGODB_SERVICE_PORT}

    if [ -z "${MONGODB_ROOT_PASSWORD}" ]; then
        export MONGODB_ALLOW_EMPTY_PASSWORD=true
    fi
}

set_env_for_replica_set() {
    export MONGODB_REPLICA_SET_NAME=${MONGODB_REPLICATION_SET_NAME}
    if [ "${IS_HEAD_NODE}" == "true" ]; then
        # Head act as primary for the first time initialization
        export MONGODB_REPLICA_SET_MODE="primary"
    else
        export MONGODB_REPLICA_SET_MODE="secondary"
        export MONGODB_INITIAL_PRIMARY_ROOT_USER="${MONGODB_ROOT_USER}"
        export MONGODB_INITIAL_PRIMARY_ROOT_PASSWORD="${MONGODB_ROOT_PASSWORD}"
        export MONGODB_INITIAL_PRIMARY_HOST=${HEAD_IP_ADDRESS}
        export MONGODB_INITIAL_PRIMARY_PORT=${MONGODB_PORT}
    fi

    if [[ -n "$MONGODB_REPLICATION_SET_KEY" ]]; then
        export MONGODB_REPLICA_SET_KEY=$MONGODB_REPLICATION_SET_KEY
        export MONGODB_KEY_FILE="$MONGODB_CONF_DIR/keyfile"
    fi
}

set_env_for_config_server() {
    set_env_for_replica_set
    export MONGODB_SHARDING_MODE="configsvr"
}

set_env_for_mongos_common() {
    export MONGODB_MONGOS_CONF_FILE="${MONGODB_MONGOS_CONFIG_FILE}"
    export MONGODB_MONGOS_PORT=${MONGODB_MONGOS_SERVICE_PORT}
    export MONGODB_MONGOS_PID_FILE="${MONGODB_HOME}/mongos.pid"
    if [[ -n "$MONGODB_REPLICATION_SET_KEY" ]]; then
        export MONGODB_REPLICA_SET_KEY="${MONGODB_REPLICATION_SET_KEY}"
        export MONGODB_KEY_FILE="$MONGODB_CONF_DIR/keyfile"
    fi
}

set_env_for_mongos_config() {
    # TODO: support list of config server hosts instead of the primary
    export MONGODB_CFG_REPLICA_SET_NAME=${MONGODB_CONFIG_SERVER_REPLICATION_SET_NAME}
    export MONGODB_CFG_PRIMARY_HOST=${MONGODB_CONFIG_SERVER_HOST}
    export MONGODB_CFG_PRIMARY_PORT="${MONGODB_CONFIG_SERVER_PORT:-${MONGODB_SERVICE_PORT}}"
}

set_env_for_mongos() {
    set_env_for_mongos_common
    export MONGODB_SHARDING_MODE="mongos"
    set_env_for_mongos_config
}

set_env_for_mongos_on_shard_server() {
    set_env_for_mongos_common
    set_env_for_mongos_config
}

set_env_for_mongos_on_config_server() {
    set_env_for_mongos_common
    # The mongos is in the same node of config server
    export MONGODB_CFG_REPLICA_SET_NAME=${MONGODB_REPLICATION_SET_NAME}
    export MONGODB_CFG_PRIMARY_HOST=${HEAD_IP_ADDRESS}
    export MONGODB_CFG_PRIMARY_PORT=${MONGODB_SERVICE_PORT}
}

set_env_for_shard() {
    set_env_for_replica_set
    export MONGODB_SHARDING_MODE="shardsvr"

    # Use mongos on cluster head ( or on the same node)
    export MONGODB_MONGOS_HOST=${HEAD_IP_ADDRESS}
    export MONGODB_MONGOS_HOST_PORT=${MONGODB_MONGOS_SERVICE_PORT}
}

configure_mongodb() {
    if [ "${IS_HEAD_NODE}" != "true" ] \
        && [ "${MONGODB_CLUSTER_MODE}" == "none" ]; then
          return
    fi

    prepare_base_conf

    mkdir -p ${MONGODB_HOME}/logs
    MONGODB_CONFIG_DIR=${MONGODB_HOME}/conf
    mkdir -p ${MONGODB_CONFIG_DIR}
    MONGODB_CONFIG_FILE=${MONGODB_CONFIG_DIR}/mongod.conf
    MONGODB_MONGOS_CONFIG_FILE=${MONGODB_CONFIG_DIR}/mongos.conf

    if [ "${MONGODB_CLUSTER_MODE}" == "replication" ]; then
        config_template_file=${output_dir}/mongod-replication.conf
        configure_mongod
        configure_replica_set
        cp ${config_template_file} ${MONGODB_CONFIG_FILE}
    elif [ "${MONGODB_CLUSTER_MODE}" == "sharding" ]; then
        if [ "${MONGODB_SHARDING_CLUSTER_ROLE}" == "mongos" ]; then
            config_template_file=${output_dir}/mongod-sharding-mongos.conf
            configure_mongos
            cp ${config_template_file} ${MONGODB_MONGOS_CONFIG_FILE}
        else
            config_template_file=${output_dir}/mongod-sharding.conf
            configure_mongod
            configure_replica_set
            cp ${config_template_file} ${MONGODB_CONFIG_FILE}

            # The mongos need on a different port number
            configure_mongos ${output_dir}/mongod-sharding-mongos.conf
            cp ${output_dir}/mongod-sharding-mongos.conf ${MONGODB_MONGOS_CONFIG_FILE}
        fi
    else
        config_template_file=${output_dir}/mongod.conf
        configure_mongod
        cp ${config_template_file} ${MONGODB_CONFIG_FILE}
    fi

    # The following environment variables are needed for mongodb-init.sh
    set_env_for_init

    # For replication set, either both MONGODB_ROOT_PASSWORD and MONGODB_REPLICA_SET_KEY
    # are set or leave both empty.
    if [ "${MONGODB_CLUSTER_MODE}" == "replication" ]; then
        set_env_for_replica_set
    elif [ "${MONGODB_CLUSTER_MODE}" == "sharding" ]; then
        if [ "${MONGODB_CLUSTER_MODE}" == "sharding" ]; then
            if [ "${MONGODB_SHARDING_CLUSTER_ROLE}" == "configsvr" ]; then
                set_env_for_config_server
                set_env_for_mongos_on_config_server
            elif [ "${MONGODB_SHARDING_CLUSTER_ROLE}" == "mongos" ]; then
                set_env_for_mongos
            else
                set_env_for_shard
                set_env_for_mongos_on_shard_server
            fi
        fi
    fi

    if [ "${MONGODB_CLUSTER_MODE}" == "sharding" ]; then
        # check and initialize the database if needed
        bash $BIN_DIR/mongodb-sharding-init.sh >${MONGODB_HOME}/logs/mongodb-init.log 2>&1
    else
        # check and initialize the database if needed
        bash $BIN_DIR/mongodb-init.sh >${MONGODB_HOME}/logs/mongodb-init.log 2>&1
    fi
}

check_mongodb_installed
set_head_option "$@"
set_node_address
set_head_address
configure_mongodb

exit 0
