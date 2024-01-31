#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/metastore/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_hive_metastore_installed() {
    if [ ! -n "${METASTORE_HOME}" ]; then
        echo "Hive Metastore is not installed."
        exit 1
    fi
}

configure_hive_metastore() {
    prepare_base_conf
    CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/hive/metastore-site.xml

    mkdir -p ${METASTORE_HOME}/logs

    if [ "${SQL_DATABASE}" != "true" ]; then
        echo "SQL database is not configured."
        exit 1
    fi

    # a standalone SQL database
    DATABASE_NAME=hive_metastore
    DATABASE_ADDRESS=${SQL_DATABASE_HOST}:${SQL_DATABASE_PORT}
    DATABASE_USER=${SQL_DATABASE_USERNAME}
    DATABASE_PASSWORD=${SQL_DATABASE_PASSWORD}
    DATABASE_ENGINE=${SQL_DATABASE_ENGINE}

    if [ "${DATABASE_ENGINE}" == "mysql" ]; then
        DATABASE_DRIVER="com.mysql.jdbc.Driver"
        DATABASE_CONNECTION="jdbc:mysql://${DATABASE_ADDRESS}/${DATABASE_NAME}?createDatabaseIfNotExist=true"
    else
        DATABASE_DRIVER="org.postgresql.Driver"
        DATABASE_CONNECTION="jdbc:postgresql://${DATABASE_ADDRESS}/${DATABASE_NAME}"
    fi

    sed -i "s/{%metastore.bind.host%}/${NODE_IP_ADDRESS}/g" ${CONFIG_TEMPLATE_FILE}
    # This is client configuration to access metastore
    sed -i "s/{%metastore.host%}/${NODE_HOST_ADDRESS}/g" ${CONFIG_TEMPLATE_FILE}

    sed -i "s#{%DATABASE_CONNECTION%}#${DATABASE_CONNECTION}#g" ${CONFIG_TEMPLATE_FILE}
    sed -i "s#{%DATABASE_DRIVER%}#${DATABASE_DRIVER}#g" ${CONFIG_TEMPLATE_FILE}
    sed -i "s/{%DATABASE_USER%}/${DATABASE_USER}/g" ${CONFIG_TEMPLATE_FILE}
    sed -i "s/{%DATABASE_PASSWORD%}/${DATABASE_PASSWORD}/g" ${CONFIG_TEMPLATE_FILE}

    # set metastore warehouse dir according to the storage options: HDFS, S3, GCS, Azure
    # The full path will be decided on the default.fs of hadoop core-site.xml
    METASTORE_WAREHOUSE_DIR=/shared/warehouse
    sed -i "s|{%metastore.warehouse.dir%}|${METASTORE_WAREHOUSE_DIR}|g" ${CONFIG_TEMPLATE_FILE}

    cp -r ${CONFIG_TEMPLATE_FILE}  ${METASTORE_HOME}/conf/metastore-site.xml
}

set_head_option "$@"
check_hive_metastore_installed
set_head_address
set_node_address
configure_hive_metastore

exit 0
