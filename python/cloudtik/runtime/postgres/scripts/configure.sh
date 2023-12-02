#!/bin/bash

# Current bin directory
BIN_DIR=`dirname "$0"`
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
POSTGRES_HOME=$RUNTIME_PATH/postgres

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

function prepare_base_conf() {
    local source_dir=$(dirname "${BIN_DIR}")/conf
    output_dir=/tmp/postgres/conf
    rm -rf  $output_dir
    mkdir -p $output_dir
    cp -r $source_dir/* $output_dir
}

function check_postgres_installed() {
    if ! command -v postgres &> /dev/null
    then
        echo "Postgres is not installed for postgres command is not available."
        exit 1
    fi
}

function update_data_dir() {
    local data_disk_dir=$(get_first_data_disk_dir)
    if [ -z "$data_disk_dir" ]; then
        data_dir="${POSTGRES_HOME}/data"
    else
        data_dir="$data_disk_dir/postgres/data"
    fi

    mkdir -p ${data_dir}
    sed -i "s#{%data.dir%}#${data_dir}#g" ${config_template_file}

    # the init script used PGDATA environment
    export PGDATA=${data_dir}
}

function update_server_id() {
    if [ ! -n "${CLOUDTIK_NODE_SEQ_ID}" ]; then
        echo "Replication needs unique server id. No node sequence id allocated for current node!"
        exit 1
    fi

    local server_id="postgres-${CLOUDTIK_NODE_SEQ_ID}"
    sed -i "s#{%server.id%}#${server_id}#g" ${config_template_file}
}

function update_synchronous_standby_names() {
    local synchronous_standby_names=""
    local standby_names=""
    # Warning: this depends on that the head node seq id = 1
    END_SERVER_ID=$((POSTGRES_SYNCHRONOUS_SIZE+1))
    for i in $(seq 2 $END_SERVER_ID); do
        if [ -z "$standby_names" ]; then
            standby_names="postgres-$i"
        else
            standby_names="$standby_names,postgres-$i"
        fi
    done

    if [ "${POSTGRES_SYNCHRONOUS_MODE}" == "first" ]; then
        synchronous_standby_names="FIRST ${POSTGRES_SYNCHRONOUS_NUM} (${standby_names})"
    elif [ "${POSTGRES_SYNCHRONOUS_MODE}" == "any" ]; then
        synchronous_standby_names="ANY ${POSTGRES_SYNCHRONOUS_NUM} (${standby_names})"
    else
        synchronous_standby_names="${standby_names}"
    fi
    sed -i "s#synchronous_standby_names = ''#synchronous_standby_names = '${synchronous_standby_names}'#g" ${config_template_file}
}

function configure_postgres() {
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

    sed -i "s#{%listen.address%}#${NODE_IP_ADDRESS}#g" ${config_template_file}
    sed -i "s#{%listen.port%}#${POSTGRES_SERVICE_PORT}#g" ${config_template_file}
    sed -i "s#{%postgres.home%}#${POSTGRES_HOME}#g" ${config_template_file}

    update_data_dir

    if [ "${POSTGRES_CLUSTER_MODE}" == "replication" ]; then
        update_server_id
    fi

    POSTGRES_CONFIG_DIR=${POSTGRES_HOME}/conf
    mkdir -p ${POSTGRES_CONFIG_DIR}
    POSTGRES_CONFIG_FILE=${POSTGRES_CONFIG_DIR}/postgresql.conf
    cp -r ${config_template_file} ${POSTGRES_CONFIG_FILE}

    # This is needed for postgres-init.sh to decide whether need to do user db setup
    # or do a base backup through the primary server.

    if [ "${IS_HEAD_NODE}" == "true" ]; then
        export POSTGRES_MASTER_NODE=true
    else
        export POSTGRES_MASTER_NODE=false
    fi

    if [ "${POSTGRES_CLUSTER_MODE}" == "replication" ]; then
        export POSTGRES_PRIMARY_HOST=${HEAD_ADDRESS}
        if [ "${POSTGRES_REPLICATION_SLOT}" == "true" ]; then
            export POSTGRES_REPLICATION_SLOT_NAME="postgres_${CLOUDTIK_NODE_SEQ_ID}"
        fi
    fi

    # check and initialize the database if needed
    bash $BIN_DIR/postgres-init.sh postgres \
        -c config_file=${POSTGRES_CONFIG_FILE} >${POSTGRES_HOME}/logs/postgres-init.log 2>&1

    if [ "${IS_HEAD_NODE}" == "true" ] && \
        [ "${POSTGRES_CLUSTER_MODE}" == "replication" ] && \
        [ "${POSTGRES_SYNCHRONOUS_MODE}" != "none" ]; then
        update_synchronous_standby_names
        # copy again the updated version
        cp -r ${config_template_file} ${POSTGRES_CONFIG_FILE}
    fi
}

set_head_option "$@"
check_postgres_installed
set_node_ip_address
set_head_address
configure_postgres

exit 0
