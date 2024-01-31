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
    OUTPUT_DIR=/tmp/mongodb/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_mongodb_installed() {
    if ! command -v mongod &> /dev/null
    then
        echo "MongoDB is not installed."
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
    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%data.dir%}" "${DATA_DIR}"
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
    local -r config_file="${1:-${CONFIG_TEMPLATE_FILE}}"
      # TODO: can bind to a hostname instead of IP if hostname is stable
    update_in_file "${config_file}" "{%bind.address%}" "${NODE_IP_ADDRESS}"
    update_in_file "${config_file}" "{%home.dir%}" "${MONGODB_HOME}"
}

configure_mongod() {
    configure_common
    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%bind.port%}" "${MONGODB_SERVICE_PORT}"
    update_data_dir
}

configure_mongos() {
    local -r config_file="${1:-${CONFIG_TEMPLATE_FILE}}"
    configure_common "${config_file}"
    update_in_file "${config_file}" "{%bind.port%}" "${MONGODB_MONGOS_SERVICE_PORT}"
}

configure_replica_set() {
    update_in_file "${CONFIG_TEMPLATE_FILE}" \
          "{%replication.set.name%}" "${MONGODB_REPLICATION_SET_NAME}"
}

configure_variable() {
    set_variable_in_file "${MONGODB_CONFIG_DIR}/mongodb" "$@"
}

set_env_for_init() {
    local mongod_dir="$( dirname -- "$(which mongod)" )"
    configure_variable MONGODB_BIN_DIR "$mongod_dir"
    configure_variable MONGODB_CONF_DIR "${MONGODB_CONFIG_DIR}"
    configure_variable MONGODB_CONF_FILE "${MONGODB_CONFIG_FILE}"
    configure_variable MONGODB_DATA_DIR "${DATA_DIR}"
    configure_variable MONGODB_PID_FILE "${MONGODB_HOME}/mongod.pid"
    configure_variable MONGODB_VOLUME_DIR "${VOLUME_DIR}"
    configure_variable MONGODB_PORT ${MONGODB_SERVICE_PORT}

    # configure ip or hostname
    if [ "${NODE_IP_ADDRESS}" != "${NODE_HOST_ADDRESS}" ]; then
        # there is hostname available
        configure_variable MONGODB_ADVERTISE_IP false
        configure_variable MONGODB_ADVERTISED_HOSTNAME "${NODE_HOST_ADDRESS}"
    else
        configure_variable MONGODB_ADVERTISE_IP true
        configure_variable MONGODB_ADVERTISED_IP "${NODE_IP_ADDRESS}"
    fi

    if [ -z "${MONGODB_ROOT_PASSWORD}" ]; then
        configure_variable MONGODB_ALLOW_EMPTY_PASSWORD true
    fi
}

set_env_for_replica_set() {
    configure_variable MONGODB_REPLICA_SET_NAME "${MONGODB_REPLICATION_SET_NAME}"
    if [ "${IS_HEAD_NODE}" == "true" ]; then
        # Head act as primary for the first time initialization
        configure_variable MONGODB_REPLICA_SET_MODE "primary"
    else
        configure_variable MONGODB_REPLICA_SET_MODE "secondary"
        configure_variable MONGODB_INITIAL_PRIMARY_ROOT_USER "${MONGODB_ROOT_USER}"
        configure_variable MONGODB_INITIAL_PRIMARY_ROOT_PASSWORD "${MONGODB_ROOT_PASSWORD}"
        configure_variable MONGODB_INITIAL_PRIMARY_HOST "${HEAD_HOST_ADDRESS}"
        configure_variable MONGODB_INITIAL_PRIMARY_PORT ${MONGODB_SERVICE_PORT}
    fi

    if [[ -n "$MONGODB_REPLICATION_SET_KEY" ]]; then
        configure_variable MONGODB_REPLICA_SET_KEY "$MONGODB_REPLICATION_SET_KEY"
        configure_variable MONGODB_KEY_FILE "$MONGODB_CONFIG_DIR/keyfile"
    fi
}

set_env_for_config_server() {
    set_env_for_replica_set
    configure_variable MONGODB_SHARDING_MODE "configsvr"
}

set_env_for_mongos_common() {
    configure_variable MONGODB_MONGOS_CONF_FILE "${MONGODB_MONGOS_CONFIG_FILE}"
    configure_variable MONGODB_MONGOS_PORT ${MONGODB_MONGOS_SERVICE_PORT}
    configure_variable MONGODB_MONGOS_PID_FILE "${MONGODB_HOME}/mongos.pid"
}

set_env_for_mongos_config() {
    # TODO: support list of config server hosts instead of the primary
    configure_variable MONGODB_CFG_REPLICA_SET_NAME "${MONGODB_CONFIG_SERVER_REPLICATION_SET_NAME}"
    configure_variable MONGODB_CFG_PRIMARY_HOST "${MONGODB_CONFIG_SERVER_HOST}"
    local cfg_server_port="${MONGODB_CONFIG_SERVER_PORT:-${MONGODB_SERVICE_PORT}}"
    configure_variable MONGODB_CFG_PRIMARY_PORT $cfg_server_port
}

set_env_for_mongos() {
    set_env_for_mongos_common
    configure_variable MONGODB_SHARDING_MODE "mongos"
    if [[ -n "$MONGODB_REPLICATION_SET_KEY" ]]; then
        configure_variable MONGODB_REPLICA_SET_KEY "${MONGODB_REPLICATION_SET_KEY}"
        configure_variable MONGODB_KEY_FILE "$MONGODB_CONFIG_DIR/keyfile"
    fi
    set_env_for_mongos_config
}

set_env_for_mongos_on_shard_server() {
    set_env_for_mongos_common
    set_env_for_mongos_config
}

set_env_for_mongos_on_config_server() {
    set_env_for_mongos_common
    # The mongos is in the same node of config server
    configure_variable MONGODB_CFG_REPLICA_SET_NAME "${MONGODB_REPLICATION_SET_NAME}"
    configure_variable MONGODB_CFG_PRIMARY_HOST "${HEAD_HOST_ADDRESS}"
    configure_variable MONGODB_CFG_PRIMARY_PORT ${MONGODB_SERVICE_PORT}
}

set_env_for_shard() {
    set_env_for_replica_set
    configure_variable MONGODB_SHARDING_MODE "shardsvr"

    # Use mongos on cluster head ( or on the same node)
    configure_variable MONGODB_MONGOS_HOST "${HEAD_HOST_ADDRESS}"
    configure_variable MONGODB_MONGOS_HOST_PORT ${MONGODB_MONGOS_SERVICE_PORT}
}

configure_service_init() {
    # The following environment variables are needed for mongodb-init.sh
    echo "# MongoDB init variables" > ${MONGODB_CONFIG_DIR}/mongodb
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
        CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/mongod-replication.conf
        configure_mongod
        configure_replica_set
        cp ${CONFIG_TEMPLATE_FILE} ${MONGODB_CONFIG_FILE}
    elif [ "${MONGODB_CLUSTER_MODE}" == "sharding" ]; then
        if [ "${MONGODB_SHARDING_CLUSTER_ROLE}" == "mongos" ]; then
            CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/mongos.conf
            configure_mongos
            cp ${CONFIG_TEMPLATE_FILE} ${MONGODB_MONGOS_CONFIG_FILE}
        else
            CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/mongod-sharding.conf
            configure_mongod
            configure_replica_set
            cp ${CONFIG_TEMPLATE_FILE} ${MONGODB_CONFIG_FILE}

            # The mongos need on a different port number
            configure_mongos ${OUTPUT_DIR}/mongos.conf
            cp ${OUTPUT_DIR}/mongos.conf ${MONGODB_MONGOS_CONFIG_FILE}
        fi
    else
        CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/mongod.conf
        configure_mongod
        cp ${CONFIG_TEMPLATE_FILE} ${MONGODB_CONFIG_FILE}
    fi

    # Set variables for export to mongodb-init.sh
    configure_service_init
}

check_mongodb_installed
set_head_option "$@"
set_node_address
set_head_address
configure_mongodb

exit 0
