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
        DATA_DIR="${MONGODB_HOME}/data"
    else
        DATA_DIR="$data_disk_dir/mongodb/data"
    fi

    mkdir -p ${DATA_DIR}
    update_in_file "${config_template_file}" "{%data.dir%}" "${DATA_DIR}"
}

update_server_id() {
    if [ ! -n "${CLOUDTIK_NODE_SEQ_ID}" ]; then
        echo "Replication needs unique server id. No node sequence id allocated for current node!"
        exit 1
    fi

    update_in_file "${config_template_file}" "{%server.id%}" "${CLOUDTIK_NODE_SEQ_ID}"
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

configure_mongodb() {
    if [ "${IS_HEAD_NODE}" != "true" ] \
        && [ "${MONGODB_CLUSTER_MODE}" == "none" ]; then
          return
    fi

    prepare_base_conf

    if [ "${MONGODB_CLUSTER_MODE}" == "replication" ]; then
        config_template_file=${output_dir}/mongod-replication.conf
    elif [ "${MONGODB_CLUSTER_MODE}" == "sharding" ]; then
        config_template_file=${output_dir}/mongod-sharding.conf
    else
        config_template_file=${output_dir}/mongod.conf
    fi

    mkdir -p ${MONGODB_HOME}/logs

    # TODO: can bind to a hostname instead of IP if hostname is stable
    update_in_file "${config_template_file}" "{%bind.address%}" "${NODE_IP_ADDRESS}"
    update_in_file "${config_template_file}" "{%bind.port%}" "${MONGODB_SERVICE_PORT}"
    update_in_file "${config_template_file}" "{%home.dir%}" "${MONGODB_HOME}"
    update_data_dir

    if [ "${MONGODB_CLUSTER_MODE}" == "replication" ]; then
        update_server_id
        update_in_file "${config_template_file}" "{%replication.set.name%}" "${MONGODB_REPLICATION_SET_NAME}"
    elif [ "${MONGODB_CLUSTER_MODE}" == "sharding" ]; then
        update_server_id
        update_in_file "${config_template_file}" "{%replication.group.name%}" "${MONGODB_SHARDING_NAME}"
    fi

    MONGODB_CONFIG_DIR=${MONGODB_HOME}/conf
    mkdir -p ${MONGODB_CONFIG_DIR}
    MONGODB_CONFIG_FILE=${MONGODB_CONFIG_DIR}/mongod.conf
    cp ${config_template_file} ${MONGODB_CONFIG_FILE}

    # The following environment variables are needed for mongodb-init.sh
    export MONGODB_BIN_DIR="$( dirname -- "${which mongod}" )"
    export MONGODB_CONF_FILE="${MONGODB_CONFIG_FILE}"
    export MONGODB_DATA_DIR="${DATA_DIR}"

    # check and initialize the database if needed
    bash $BIN_DIR/mongodb-init.sh >${MONGODB_HOME}/logs/mongo-init.log 2>&1
}

check_mongodb_installed
set_head_option "$@"
set_node_address
set_head_address
configure_mongodb

exit 0
