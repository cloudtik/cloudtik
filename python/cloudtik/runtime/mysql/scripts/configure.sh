#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
MYSQL_HOME=$RUNTIME_PATH/mysql

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/mysql/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_mysql_installed() {
    if ! command -v mysqld &> /dev/null
    then
        echo "MySQL is not installed."
        exit 1
    fi
}

update_data_dir() {
    local data_disk_dir=$(get_first_data_disk_dir)
    if [ -z "$data_disk_dir" ]; then
        data_dir="${MYSQL_HOME}/data"
    else
        data_dir="$data_disk_dir/mysql/data"
    fi

    mkdir -p ${data_dir}
    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%data.dir%}" "${data_dir}"

    if [ "${MYSQL_CLUSTER_MODE}" == "group_replication" ]; then
        update_in_file "${OUTPUT_DIR}/my-init.cnf" "{%data.dir%}" "${data_dir}"
    fi
}

update_server_id() {
    if [ ! -n "${CLOUDTIK_NODE_SEQ_ID}" ]; then
        echo "Replication needs unique server id. No node sequence id allocated for current node!"
        exit 1
    fi

    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%server.id%}" "${CLOUDTIK_NODE_SEQ_ID}"
}

configure_variable() {
    set_variable_in_file "${MYSQL_CONFIG_DIR}/mysql" "$@"
}

configure_service_init() {
    echo "# MySQL init variables" > ${MYSQL_CONFIG_DIR}/mysql

    configure_variable MYSQL_CONF_FILE "${MYSQL_CONFIG_FILE}"
    configure_variable MYSQL_MASTER_NODE ${IS_HEAD_NODE}
    configure_variable MYSQL_CLUSTER_MODE "${MYSQL_CLUSTER_MODE}"
    configure_variable MYSQL_PORT "${MYSQL_SERVICE_PORT}"

    if [ "${MYSQL_CLUSTER_MODE}" == "replication" ]; then
        configure_variable MYSQL_REPLICATION_SOURCE_HOST "${HEAD_HOST_ADDRESS}"
    elif [ "${MYSQL_CLUSTER_MODE}" == "group_replication" ]; then
        configure_variable MYSQL_INIT_DATADIR_CONF "${MYSQL_CONFIG_DIR}/my-init.cnf"
    fi

    # TODO: further improve the security of the password in file
    configure_variable MYSQL_ROOT_PASSWORD "${MYSQL_ROOT_PASSWORD}"

    # make it owner only read/write for security
    chmod 0600 "${MYSQL_CONFIG_DIR}/mysql"
}

configure_mysql() {
    if [ "${IS_HEAD_NODE}" != "true" ] \
        && [ "${MYSQL_CLUSTER_MODE}" == "none" ]; then
          return
    fi

    prepare_base_conf

    if [ "${MYSQL_CLUSTER_MODE}" == "replication" ]; then
        CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/my-replication.cnf
    elif [ "${MYSQL_CLUSTER_MODE}" == "group_replication" ]; then
        CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/my-group-replication.cnf
    else
        CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/my.cnf
    fi

    mkdir -p ${MYSQL_HOME}/logs

    # ensure that /var/run/mysqld (used for socket and lock files) is writable
    # regardless of the UID our mysqld instance ends up having at runtime
    sudo mkdir -p /var/run/mysqld \
    && sudo chown -R $(whoami):$(id -gn) /var/run/mysqld \
    && sudo chmod 1777 /var/run/mysqld

    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%bind.address%}" "${NODE_IP_ADDRESS}"
    update_in_file "${CONFIG_TEMPLATE_FILE}" "{%bind.port%}" "${MYSQL_SERVICE_PORT}"
    update_data_dir

    if [ "${MYSQL_CLUSTER_MODE}" == "replication" ]; then
        update_server_id
    elif [ "${MYSQL_CLUSTER_MODE}" == "group_replication" ]; then
        update_server_id
        update_in_file "${CONFIG_TEMPLATE_FILE}" \
          "{%group.replication.group.name%}" "${MYSQL_GROUP_REPLICATION_NAME}"
        update_in_file "${CONFIG_TEMPLATE_FILE}" \
          "{%group.replication.local.host%}" "${NODE_HOST_ADDRESS}"
        update_in_file "${CONFIG_TEMPLATE_FILE}" \
          "{%group.replication.port%}" "${MYSQL_GROUP_REPLICATION_PORT}"

        # set head address as seed address is good for first start
        # But if head is dead while other workers are running, we need head start using workers as seeds
        # This is done at service start for head.
        # While for workers, we can always trust there is a healthy head to contact with.
        update_in_file "${CONFIG_TEMPLATE_FILE}" \
          "{%group.replication.group.seeds%}" "${HEAD_HOST_ADDRESS}:${MYSQL_GROUP_REPLICATION_PORT}"

        if [ "${MYSQL_GROUP_REPLICATION_MULTI_PRIMARY}" == "true" ]; then
            # turn on a few flags for multi-primary mode
            local config_name="group_replication_single_primary_mode"
            update_in_file "${CONFIG_TEMPLATE_FILE}" "^${config_name}=ON" "${config_name}=OFF"

            config_name="group_replication_enforce_update_everywhere_checks"
            update_in_file "${CONFIG_TEMPLATE_FILE}" "^${config_name}=OFF" "${config_name}=ON"
        fi
    fi

    MYSQL_CONFIG_DIR=${MYSQL_HOME}/conf
    mkdir -p ${MYSQL_CONFIG_DIR}
    MYSQL_CONFIG_FILE=${MYSQL_CONFIG_DIR}/my.cnf
    cp -r ${CONFIG_TEMPLATE_FILE} ${MYSQL_CONFIG_FILE}

    if [ "${MYSQL_CLUSTER_MODE}" == "group_replication" ]; then
        # This is needed because mysqld --initialize(-insecure) cannot recognize
        # many group replications options in the conf file (plugin is not loaded
        # for initialize process) and also we need to skip all bin log during this
        # process.
        cp ${OUTPUT_DIR}/my-init.cnf ${MYSQL_CONFIG_DIR}/my-init.cnf
    fi

    # Set variables for export to mysql-init.sh
    configure_service_init
}

check_mysql_installed
set_head_option "$@"
set_node_address
set_head_address
configure_mysql

exit 0
