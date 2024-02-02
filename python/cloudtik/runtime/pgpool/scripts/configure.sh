#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
PGPOOL_HOME=$RUNTIME_PATH/pgpool

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

# Pgpool functions
. "$BIN_DIR"/pgpool.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/pgpool/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_pgpool_installed() {
    if ! command -v pgpool &> /dev/null
    then
        echo "Pgpool is not installed."
        exit 1
    fi
}

update_place_holder() {
    local -r text_place_holder="${1:?text place holder is required}"
    local -r text_value="${2:-}"
    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%${text_place_holder}%}" "${text_value}"
}

configure_variable() {
    set_variable_in_file "${PGPOOL_CONFIG_DIR}/pgpool" "$@"
}

configure_service_init() {
    local -r var_file=${PGPOOL_CONFIG_DIR}/pgpool
    echo "# Pgpool init variables" > $var_file

    configure_variable PGPOOL_CONF_FILE "${PGPOOL_CONFIG_FILE}"
    configure_variable PGPOOL_PCP_FILE "${PGPOOL_CONFIG_DIR}/pcp.conf"
    configure_variable PGPOOL_HBA_FILE "${PGPOOL_CONFIG_DIR}/pool_hba.conf"
    configure_variable PGPOOL_AUTHENTICATION_METHOD "${PGPOOL_AUTHENTICATION_METHOD:-scram-sha-256}"
    configure_variable PGPOOLKEYFILE "${PGPOOL_CONFIG_DIR}/.pgpoolkey"

    configure_variable PGPOOL_PORT ${PGPOOL_SERVICE_PORT}
    configure_variable PGPOOL_PCP_PORT ${PGPOOL_PCP_PORT}
    # TODO: further improve the security of the password in file
    configure_variable PGPOOL_ADMIN_USER "${PGPOOL_ADMIN_USER}"
    configure_variable PGPOOL_ADMIN_PASSWORD "${PGPOOL_ADMIN_PASSWORD}"
    configure_variable PGPOOL_POSTGRES_USER "${PGPOOL_POSTGRES_USER}"
    configure_variable PGPOOL_POSTGRES_PASSWORD "${PGPOOL_POSTGRES_PASSWORD}"

    # make it owner only read/write for security
    chmod 0600 "$var_file"
}

configure_pgpool() {
    prepare_base_conf
    CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/pgpool.conf

    mkdir -p ${PGPOOL_HOME}/logs
    mkdir -p ${PGPOOL_HOME}/run
    mkdir -p ${PGPOOL_HOME}/data

    PGPOOL_CONFIG_DIR=${PGPOOL_HOME}/conf
    mkdir -p ${PGPOOL_CONFIG_DIR}
    PGPOOL_CONFIG_FILE="${PGPOOL_CONFIG_DIR}/pgpool.conf"

    # The listen_addresses cannot be ip address list. So we bind all.
    # update_place_holder "listen.address" "${NODE_IP_ADDRESS}"
    update_place_holder "listen.port" "${PGPOOL_SERVICE_PORT}"
    update_place_holder "pcp.port" "${PGPOOL_PCP_PORT}"
    update_place_holder "pgpool.home" "${PGPOOL_HOME}"

    # max.pool
    update_place_holder "max.pool" "${PGPOOL_MAX_POOL}"

    update_place_holder "sr.check.user" "${PGPOOL_REPLICATION_USER}"
    update_place_holder "sr.check.password" "${PGPOOL_REPLICATION_PASSWORD}"
    update_place_holder "health.check.user" "${PGPOOL_REPLICATION_USER}"
    update_place_holder "health.check.password" "${PGPOOL_REPLICATION_PASSWORD}"

    cp $CONFIG_TEMPLATE_FILE ${PGPOOL_CONFIG_FILE}
    # make it owner only read/write for security
    chmod 0600 "${PGPOOL_CONFIG_FILE}"

    cp ${OUTPUT_DIR}/pcp.conf ${PGPOOL_CONFIG_DIR}/pcp.conf
    cp ${OUTPUT_DIR}/pool_hba.conf ${PGPOOL_CONFIG_DIR}/pool_hba.conf

    # Set variables for export to postgres-init.sh
    configure_service_init

    # generate the pool_passwd file after the conf is prepared and copied
    . ${PGPOOL_CONFIG_DIR}/pgpool
    pgpool_generate_password_file
    pgpool_generate_admin_password_file
    pgpool_create_pghba
}

set_head_option "$@"
check_pgpool_installed
set_head_address
set_node_address
configure_pgpool

exit 0
