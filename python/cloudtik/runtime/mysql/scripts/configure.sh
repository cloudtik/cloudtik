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
    local source_dir=$(dirname "${BIN_DIR}")/conf
    output_dir=/tmp/mysql/conf
    rm -rf  $output_dir
    mkdir -p $output_dir
    cp -r $source_dir/* $output_dir
}

check_mysql_installed() {
    if ! command -v mysqld &> /dev/null
    then
        echo "MySQL is not installed for mysqld command is not available."
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
    update_in_file "${config_template_file}" "{%data.dir%}" "${data_dir}"

    if [ "${MYSQL_CLUSTER_MODE}" == "group_replication" ]; then
        update_in_file "${output_dir}/my-init.cnf" "{%data.dir%}" "${data_dir}"
    fi
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
        # only do this for workers for now, head needs handle differently for group replication
        if [ "${MYSQL_CLUSTER_MODE}" == "replication" ]; then
            update_in_file "${MYSQL_CONFIG_FILE}" "^skip_replica_start=ON" "skip_replica_start=OFF"
        elif [ "${MYSQL_CLUSTER_MODE}" == "group_replication" ]; then
            update_in_file "${MYSQL_CONFIG_FILE}" "^group_replication_start_on_boot=OFF" "group_replication_start_on_boot=ON"
        fi
    fi
}

configure_mysql() {
    if [ "${IS_HEAD_NODE}" != "true" ] \
        && [ "${MYSQL_CLUSTER_MODE}" == "none" ]; then
          return
    fi

    prepare_base_conf

    if [ "${MYSQL_CLUSTER_MODE}" == "replication" ]; then
        config_template_file=${output_dir}/my-replication.cnf
    elif [ "${MYSQL_CLUSTER_MODE}" == "group_replication" ]; then
        config_template_file=${output_dir}/my-group-replication.cnf
    else
        config_template_file=${output_dir}/my.cnf
    fi

    mkdir -p ${MYSQL_HOME}/logs

    # ensure that /var/run/mysqld (used for socket and lock files) is writable
    # regardless of the UID our mysqld instance ends up having at runtime
    sudo mkdir -p /var/run/mysqld \
    && sudo chown -R $(whoami):$(id -gn) /var/run/mysqld \
    && sudo chmod 1777 /var/run/mysqld

    update_in_file "${config_template_file}" "{%bind.address%}" "${NODE_IP_ADDRESS}"
    update_in_file "${config_template_file}" "{%bind.port%}" "${MYSQL_SERVICE_PORT}"
    update_data_dir

    if [ "${MYSQL_CLUSTER_MODE}" == "replication" ]; then
        update_server_id
    elif [ "${MYSQL_CLUSTER_MODE}" == "group_replication" ]; then
        update_server_id
        update_in_file "${config_template_file}" "{%group.replication.group.name%}" "${MYSQL_GROUP_REPLICATION_NAME}"
        update_in_file "${config_template_file}" "{%group.replication.port%}" "${MYSQL_GROUP_REPLICATION_PORT}"

        # TODO: set head address as seed address is good for first start
        # But if head is dead while other workers are running, we need head start using workers as seeds
        # This need to be improved with fixed naming services if we know a fixed number of nodes. We can
        # assume that the first N nodes used as seeds.
        # While for workers, we can always trust there is a healthy head to contact with.
        update_in_file "${config_template_file}" "{%group.replication.seed.address%}" "${HEAD_IP_ADDRESS}"

        if [ "${MYSQL_GROUP_REPLICATION_MULTI_PRIMARY}" == "true" ]; then
            # turn on a few flags for multi-primary mode
            local config_name="group_replication_single_primary_mode"
            update_in_file "${config_template_file}" "^${config_name}=ON" "${config_name}=OFF"

            config_name="group_replication_enforce_update_everywhere_checks"
            update_in_file "${config_template_file}" "^${config_name}=OFF" "${config_name}=ON"
        fi
    fi

    MYSQL_CONFIG_DIR=${MYSQL_HOME}/conf
    mkdir -p ${MYSQL_CONFIG_DIR}
    MYSQL_CONFIG_FILE=${MYSQL_CONFIG_DIR}/my.cnf
    cp -r ${config_template_file} ${MYSQL_CONFIG_FILE}

    # This is needed for mysql-init.sh to decide whether need to do user db setup

    if [ "${IS_HEAD_NODE}" == "true" ]; then
        # export for mysql_init.sh
        export MYSQL_MASTER_NODE=true
    else
        export MYSQL_MASTER_NODE=false
    fi

    if [ "${MYSQL_CLUSTER_MODE}" == "replication" ]; then
        export MYSQL_REPLICATION_SOURCE_HOST=${HEAD_IP_ADDRESS}
    elif [ "${MYSQL_CLUSTER_MODE}" == "group_replication" ]; then
        # This is needed because mysqld --initialize(-insecure) cannot recognize
        # many group replications options in the conf file (plugin is not loaded
        # for initialize process) and also we need to skip all bin log during this
        # process.
        cp ${output_dir}/my-init.cnf ${MYSQL_CONFIG_DIR}/my-init.cnf
        export MYSQL_INIT_DATADIR_CONF=${MYSQL_CONFIG_DIR}/my-init.cnf
    fi

    # check and initialize the database if needed
    bash $BIN_DIR/mysql-init.sh mysqld \
        --defaults-file=${MYSQL_CONFIG_FILE} >${MYSQL_HOME}/logs/mysql-init.log 2>&1

    turn_on_start_replication_on_boot
}

check_mysql_installed
set_head_option "$@"
set_node_address
set_head_address
configure_mysql

exit 0
