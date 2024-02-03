#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
PGBOUNCER_HOME=$RUNTIME_PATH/pgbouncer

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/pgbouncer/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_pgbouncer_installed() {
    if ! command -v pgbouncer &> /dev/null
    then
        echo "PgBouncer is not installed."
        exit 1
    fi
}

update_place_holder() {
    local -r text_place_holder="${1:?text place holder is required}"
    local -r text_value="${2:-}"
    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%${text_place_holder}%}" "${text_value}"
}

configure_variable() {
    set_variable_in_file "${PGBOUNCER_CONFIG_DIR}/pgbouncer" "$@"
}

configure_service_init() {
    local -r var_file=${PGBOUNCER_CONFIG_DIR}/pgbouncer
    echo "# PgBouncer init variables" > $var_file

    configure_variable PGBOUNCER_CONF_FILE "${PGBOUNCER_CONFIG_FILE}"
    configure_variable PGBOUNCER_PORT ${PGBOUNCER_SERVICE_PORT}
    configure_variable PGBOUNCER_PID_FILE "${PGBOUNCER_HOME}/run/pgbouncer.pid"

    # make it owner only read/write for security
    chmod 0600 "$var_file"
}

configure_pgbouncer() {
    prepare_base_conf
    CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/pgbouncer.ini

    mkdir -p ${PGBOUNCER_HOME}/logs
    mkdir -p ${PGBOUNCER_HOME}/run

    PGBOUNCER_CONFIG_DIR=${PGBOUNCER_HOME}/conf
    mkdir -p ${PGBOUNCER_CONFIG_DIR}
    PGBOUNCER_CONFIG_FILE=${PGBOUNCER_CONFIG_DIR}/pgbouncer.ini

    # The listen_addresses cannot be ip address list. So we bind all.
    update_place_holder "listen.ip" "${NODE_IP_ADDRESS}"
    update_place_holder "listen.port" "${PGBOUNCER_SERVICE_PORT}"
    update_place_holder "pgbouncer.home" "${PGBOUNCER_HOME}"
    update_place_holder "pool.mode" "${PGBOUNCER_POOL_MODE}"
    update_place_holder "admin.user" "${PGBOUNCER_ADMIN_USER}"

    # pool sizes
    update_place_holder "default.pool.size" "${PGBOUNCER_POOL_SIZE}"
    update_place_holder "min.pool.size" "${PGBOUNCER_MIN_POOL_SIZE}"
    update_place_holder "reserve.pool.size" "${PGBOUNCER_RESERVE_POOL_SIZE}"

    cp $CONFIG_TEMPLATE_FILE ${PGBOUNCER_CONFIG_FILE}
    # make it owner only read/write for security
    chmod 0600 "${PGBOUNCER_CONFIG_FILE}"
    # The template file is use to append backend databases at the end
    cp ${PGBOUNCER_CONFIG_FILE} ${PGBOUNCER_CONFIG_DIR}/pgbouncer-template.ini

    # Set variables for export to postgres-init.sh
    configure_service_init
}

set_head_option "$@"
check_pgbouncer_installed
set_head_address
set_node_address
configure_pgbouncer

exit 0
