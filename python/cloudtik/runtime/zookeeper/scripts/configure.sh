#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/zookeeper/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_zookeeper_installed() {
    if [ ! -n "${ZOOKEEPER_HOME}" ]; then
        echo "ZooKeeper is not installed."
        exit 1
    fi
}

update_zookeeper_data_disks_config() {
    local data_disk_dir=$(get_first_data_disk_dir)
    if [ -z "$data_disk_dir" ]; then
        zookeeper_data_dir="${ZOOKEEPER_HOME}/data"
    else
        zookeeper_data_dir="$data_disk_dir/zookeeper/data"
    fi

    mkdir -p $zookeeper_data_dir
    sed -i "s!{%zookeeper.dataDir%}!${zookeeper_data_dir}!g" ${OUTPUT_DIR}/zookeeper/zoo.cfg
}

update_myid() {
    # Configure my id file
    if [ ! -n "${CLOUDTIK_NODE_SEQ_ID}" ]; then
        echo "No node sequence id allocated for current node!"
        exit 1
    fi

    sed -i "s!{%zookeeper.myid%}!${CLOUDTIK_NODE_SEQ_ID}!g" ${OUTPUT_DIR}/zookeeper/myid
}

configure_zookeeper() {
    prepare_base_conf
    mkdir -p ${ZOOKEEPER_HOME}/logs

    update_zookeeper_data_disks_config
    # Zookeeper server ensemble will be updated in up-level of configure
    update_myid

    cp -r ${OUTPUT_DIR}/zookeeper/zoo.cfg  ${ZOOKEEPER_HOME}/conf/zoo.cfg
    cp -r ${OUTPUT_DIR}/zookeeper/myid  $zookeeper_data_dir/myid
}

set_head_option "$@"

if [ "$IS_HEAD_NODE" == "false" ]; then
    # Zookeeper doesn't run on head node
    check_zookeeper_installed
    set_head_address
    configure_zookeeper
fi

exit 0
