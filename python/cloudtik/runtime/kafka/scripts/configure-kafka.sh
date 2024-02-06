#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l "head::,zookeeper_connect:" -- "$@")
eval set -- "${args}"

IS_HEAD_NODE=false
USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

set_zookeeper_connect() {
    while true
    do
        case "$1" in
        --zookeeper_connect)
            ZOOKEEPER_CONNECT=$2
            shift
            ;;
        --)
            shift
            break
            ;;
        esac
        shift
    done
}

prepare_base_conf() {
    OUTPUT_DIR=/tmp/kafka/conf
    local source_dir=$(cd $(dirname ${BASH_SOURCE[0]})/..;pwd)/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_kafka_installed() {
    if [ ! -n "${KAFKA_HOME}" ]; then
        echo "Kafka is not installed."
        exit 1
    fi
}

update_kafka_data_disks_config() {
    local kafka_data_dir=$(get_data_disk_dirs_of "kafka/data" true)
    # if no disks mounted
    if [ -z "$kafka_data_dir" ]; then
        kafka_data_dir="${KAFKA_HOME}/data"
        mkdir -p $kafka_data_dir
    fi

    sed -i "s!{%kafka.log.dir%}!${kafka_data_dir}!g" ${OUTPUT_DIR}/kafka/server.properties
}

update_broker_id() {
    # Configure my id file
    if [ ! -n "${CLOUDTIK_NODE_SEQ_ID}" ]; then
        echo "No node sequence id allocated for current node!"
        exit 1
    fi

    sed -i "s!{%kafka.broker.id%}!${CLOUDTIK_NODE_SEQ_ID}!g" ${OUTPUT_DIR}/kafka/server.properties
}

update_zookeeper_connect() {
    # Update zookeeper connect
    if [ -z "$ZOOKEEPER_CONNECT" ]; then
        echo "No zookeeper connect parameter specified!"
        exit 1
    fi

    sed -i "s!{%kafka.zookeeper.connect%}!${ZOOKEEPER_CONNECT}!g" ${OUTPUT_DIR}/kafka/server.properties
}

update_listeners() {
    kafka_listeners="PLAINTEXT://${NODE_IP_ADDRESS}:9092"
    sed -i "s!{%kafka.listeners%}!${kafka_listeners}!g" ${OUTPUT_DIR}/kafka/server.properties
}

configure_kafka() {
    prepare_base_conf
    mkdir -p ${KAFKA_HOME}/logs

    update_broker_id
    update_listeners
    update_zookeeper_connect
    update_kafka_data_disks_config

    cp -r ${OUTPUT_DIR}/kafka/server.properties ${KAFKA_HOME}/config/server.properties
}

set_head_option "$@"
set_zookeeper_connect "$@"

if [ "$IS_HEAD_NODE" == "false" ];then
    # Zookeeper doesn't run on head node
    check_kafka_installed
    set_head_address
    set_node_address
    configure_kafka
fi

exit 0
