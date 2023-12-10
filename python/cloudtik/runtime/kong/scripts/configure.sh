#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
KONG_HOME=$RUNTIME_PATH/kong

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    local source_dir=$(dirname "${BIN_DIR}")/conf
    output_dir=/tmp/kong/conf
    rm -rf  $output_dir
    mkdir -p $output_dir
    cp -r $source_dir/* $output_dir
}

check_kong_installed() {
    if ! command -v kong &> /dev/null
    then
        echo "Kong is not installed for kong command is not available."
        exit 1
    fi
}

configure_database() {
    DATABASE_NAME=kong
    DATABASE_USER=kong
    # TODO: allow user to specify the database password
    DATABASE_PASSWORD=kong
    if [ "${SQL_DATABASE}" == "true" ]; then
        # a standalone SQL database
        DATABASE_HOST=${SQL_DATABASE_HOST}
        DATABASE_PORT=${SQL_DATABASE_PORT}
        DATABASE_ENGINE=${SQL_DATABASE_ENGINE}
    else
        echo "ERROR: No SQL database configured."
        exit 1
    fi

    sed -i "s#{%database.host%}#${DATABASE_HOST}#g" ${config_template_file}
    sed -i "s#{%database.port%}#${DATABASE_PORT}#g" ${config_template_file}
    sed -i "s/{%database.name%}/${DATABASE_NAME}/g" ${config_template_file}
    sed -i "s/{%database.user%}/${DATABASE_USER}/g" ${config_template_file}
    sed -i "s/{%database.password%}/${DATABASE_PASSWORD}/g" ${config_template_file}
}

configure_kong() {
    prepare_base_conf
    mkdir -p ${KONG_HOME}/logs

    KONG_CONF_DIR=${KONG_HOME}/conf
    mkdir -p ${KONG_CONF_DIR}

    config_template_file=${output_dir}/kong.conf

    sed -i "s#{%listen.ip%}#${NODE_IP_ADDRESS}#g" ${config_template_file}
    sed -i "s#{%listen.port%}#${KONG_SERVICE_PORT}#g" ${config_template_file}
    sed -i "s#{%listen.ssl.port%}#${KONG_SERVICE_SSL_PORT}#g" ${config_template_file}
    sed -i "s#{%admin.port%}#${KONG_ADMIN_PORT}#g" ${config_template_file}
    sed -i "s#{%admin.ssl.port%}#${KONG_ADMIN_SSL_PORT}#g" ${config_template_file}
    sed -i "s#{%admin.ui.port%}#${KONG_ADMIN_UI_PORT}#g" ${config_template_file}
    sed -i "s#{%admin.ui.ssl.port%}#${KONG_ADMIN_UI_SSL_PORT}#g" ${config_template_file}

    # may need to configure in the future for high availability clustering
    # refer to: https://docs.konghq.com/gateway/latest/production/clustering/

    configure_database

    cp ${config_template_file} ${KONG_CONF_DIR}/kong.conf
}

set_head_option "$@"
check_kong_installed
set_node_address
configure_kong

exit 0
