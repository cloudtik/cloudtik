#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
MINIO_HOME=$RUNTIME_PATH/minio

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

check_minio_installed() {
    if [ ! -f "${MINIO_HOME}/bin/minio" ]; then
        echo "MinIO is not installed."
        exit 1
    fi
}

prepare_base_conf() {
    OUTPUT_DIR=/tmp/minio/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

prepare_data_disks() {
    local minio_data_dirs=""
    if [ -d "/mnt/cloudtik" ]; then
        for data_disk in /mnt/cloudtik/*; do
            [ -d "$data_disk" ] || continue
            local data_dir="$data_disk/minio"
            if [ "$MINIO_FORCE_CLEAN" == "true" ]; then
                sudo rm -rf "$data_dir"
            fi
            mkdir -p "$data_dir"
            if [ -z "$minio_data_dirs" ]; then
                minio_data_dirs="$data_dir"
            else
                minio_data_dirs="$minio_data_dirs,$data_dir"
            fi
        done
    fi

    # if no disks mounted
    if [ -z "$minio_data_dirs" ]; then
        minio_data_dirs="${MINIO_HOME}/data"
        if [ "$MINIO_FORCE_CLEAN" == "true" ]; then
            sudo rm -rf "$minio_data_dirs"
        fi
        mkdir -p "$minio_data_dirs"
    fi
}

configure_minio() {
    prepare_base_conf
    CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/minio

    mkdir -p ${MINIO_HOME}/conf
    mkdir -p ${MINIO_HOME}/logs

    if [ "${IS_HEAD_NODE}" != "true" ] \
        || [ "${MINIO_SERVICE_ON_HEAD}" != "false" ]; then
        prepare_data_disks

        sed -i "s#{%bind.ip%}#${NODE_IP_ADDRESS}#g" ${CONFIG_TEMPLATE_FILE}
        sed -i "s#{%service.port%}#${MINIO_SERVICE_PORT}#g" ${CONFIG_TEMPLATE_FILE}
        sed -i "s#{%console.port%}#${MINIO_CONSOLE_PORT}#g" ${CONFIG_TEMPLATE_FILE}
        sed -i "s#{%minio.volumes%}#${MINIO_VOLUMES}#g" ${CONFIG_TEMPLATE_FILE}

        cp -r ${CONFIG_TEMPLATE_FILE} ${MINIO_HOME}/conf/minio
    fi
}

set_head_option "$@"
check_minio_installed
configure_minio

exit 0
