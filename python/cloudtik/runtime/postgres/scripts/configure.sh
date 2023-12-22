#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
POSTGRES_HOME=$RUNTIME_PATH/postgres

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    local source_dir=$(dirname "${BIN_DIR}")/conf
    output_dir=/tmp/postgres/conf
    rm -rf  $output_dir
    mkdir -p $output_dir
    cp -r $source_dir/* $output_dir
}

check_postgres_installed() {
    if ! command -v postgres &> /dev/null
    then
        echo "Postgres is not installed."
        exit 1
    fi
}

update_data_dir() {
    local data_disk_dir=$(get_first_data_disk_dir)
    if [ -z "$data_disk_dir" ]; then
        data_dir="${POSTGRES_HOME}/data"
    else
        data_dir="$data_disk_dir/postgres/data"
    fi

    mkdir -p ${data_dir}
    update_in_file "${config_template_file}" "{%data.dir%}" "${data_dir}"

    POSTGRES_DATA_DIR=${data_dir}
}

update_server_id() {
    if [ ! -n "${CLOUDTIK_NODE_SEQ_ID}" ]; then
        echo "Replication needs unique server id. No node sequence id allocated for current node!"
        exit 1
    fi

    local server_id="postgres_${CLOUDTIK_NODE_SEQ_ID}"
    update_in_file "${config_template_file}" "{%server.id%}" "${server_id}"
}

configure_archive() {
    # turn on archive mode
    update_in_file "${config_template_file}" "archive_mode = off" "archive_mode = on"

    # update the archive_command (need escape && for sed)
    local archive_command="test ! -f ${ARCHIVE_DIR}/%f \&\& cp %p ${ARCHIVE_DIR}/%f"
    update_in_file "${config_template_file}" "archive_command = ''" "archive_command = '${archive_command}'"
}

configure_restore_command() {
    # update the restore_command
    local restore_command="cp ${ARCHIVE_DIR}/%f %p"
    update_in_file "${config_template_file}" "restore_command = ''" "restore_command = '${restore_command}'"
}

configure_variable() {
    set_variable_in_file "${POSTGRES_CONFIG_DIR}/postgres" "$@"
}

configure_service_init() {
    echo "# Postgres init variables" > ${POSTGRES_CONFIG_DIR}/postgres

    configure_variable POSTGRES_CONF_FILE "${POSTGRES_CONFIG_FILE}"
    # the init script used PGDATA environment
    configure_variable PGDATA "${POSTGRES_DATA_DIR}"
    configure_variable POSTGRES_MASTER_NODE "${IS_HEAD_NODE}"

    if [ "${POSTGRES_CLUSTER_MODE}" == "replication" ]; then
        configure_variable POSTGRES_PRIMARY_HOST "${HEAD_HOST_ADDRESS}"
        if [ "${POSTGRES_REPLICATION_SLOT}" == "true" ]; then
            configure_variable POSTGRES_REPLICATION_SLOT_NAME "postgres_${CLOUDTIK_NODE_SEQ_ID}"
        fi
    fi
}

configure_postgres() {
    if [ "${IS_HEAD_NODE}" != "true" ] \
        && [ "${POSTGRES_CLUSTER_MODE}" == "none" ]; then
          return
    fi

    prepare_base_conf

    if [ "${POSTGRES_CLUSTER_MODE}" == "replication" ]; then
        config_template_file=${output_dir}/postgresql-replication.conf
    else
        config_template_file=${output_dir}/postgresql.conf
    fi

    mkdir -p ${POSTGRES_HOME}/logs

    sudo mkdir -p /var/run/postgresql \
    && sudo chown -R $(whoami):$(id -gn) /var/run/postgresql \
    && sudo chmod 2777 /var/run/postgresql

    update_in_file "${config_template_file}" "{%listen.address%}" "${NODE_IP_ADDRESS}"
    update_in_file "${config_template_file}" "{%listen.port%}" "${POSTGRES_SERVICE_PORT}"
    update_in_file "${config_template_file}" "{%postgres.home%}" "${POSTGRES_HOME}"

    update_data_dir

    if [ "${POSTGRES_CLUSTER_MODE}" == "replication" ]; then
        update_server_id
    fi

    if [ "${POSTGRES_ARCHIVE_MODE}" == "true" ]; then
        # NOTE: create the folder before the starting of the service
        ARCHIVE_DIR="/cloudtik/fs/postgres/archives/${CLOUDTIK_CLUSTER}"
        configure_archive
        if [ "${IS_HEAD_NODE}" != "true" ]; then
            configure_restore_command
        fi
    fi

    POSTGRES_CONFIG_DIR=${POSTGRES_HOME}/conf
    mkdir -p ${POSTGRES_CONFIG_DIR}
    POSTGRES_CONFIG_FILE=${POSTGRES_CONFIG_DIR}/postgresql.conf
    cp -r ${config_template_file} ${POSTGRES_CONFIG_FILE}

    # Set variables for export to postgres-init.sh
    configure_service_init
}

set_head_option "$@"
check_postgres_installed
set_node_address
set_head_address
configure_postgres

exit 0
