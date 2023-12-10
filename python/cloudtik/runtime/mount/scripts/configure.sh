#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
MOUNT_HOME=$USER_HOME/runtime/mount

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

# storage mount functions
. "$BIN_DIR"/mount-storage.sh

prepare_base_conf() {
    source_dir=$(dirname "${BIN_DIR}")/conf
    output_dir=/tmp/mount/conf
    rm -rf  $output_dir
    mkdir -p $output_dir
    cp -r $source_dir/* $output_dir
}

export_configurations() {
    local conf_file=${output_dir}/mount.conf
    if [ ! -z "${HDFS_NAMENODE_URI}" ]; then
        echo "HDFS_NAMENODE_URI=${HDFS_NAMENODE_URI}">> $conf_file
    fi
    if [ ! -z "${MINIO_ENDPOINT_URI}" ]; then
        echo "MINIO_ENDPOINT_URI=${MINIO_ENDPOINT_URI}">> $conf_file
    fi
    if [ ! -z "${MINIO_BUCKET}" ]; then
        echo "MINIO_BUCKET=${MINIO_BUCKET}">> $conf_file
    fi
}

configure_mount() {
    prepare_base_conf

    mkdir -p $MOUNT_HOME/conf
    export_configurations
    cp ${output_dir}/mount.conf $MOUNT_HOME/conf/mount.conf

    configure_storage_fs
}

set_head_option "$@"
set_head_address
configure_mount

exit 0
