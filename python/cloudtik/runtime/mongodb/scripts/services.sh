#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

# import util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

set_head_option "$@"
set_service_command "$@"
set_node_address
set_head_address

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
MONGODB_HOME=$RUNTIME_PATH/mongodb

start_mongod() {
    MONGODB_CONFIG_FILE=${MONGODB_HOME}/conf/mongod.conf
    mongod --fork \
        --config ${MONGODB_CONFIG_FILE} \
        >${MONGODB_HOME}/logs/mongod-start.log 2>&1
}

stop_mongod() {
  local pid_file=${MONGODB_HOME}/mongod.pid
  stop_process_by_pid_file "${pid_file}"
}

start_mongos() {
    MONGODB_MONGOS_CONFIG_FILE=${MONGODB_HOME}/conf/mongos.conf
    mongos --fork \
    --config ${MONGODB_MONGOS_CONFIG_FILE} \
    >${MONGODB_HOME}/logs/mongos-start.log 2>&1
}

stop_mongos() {
  local pid_file=${MONGODB_HOME}/mongos.pid
  stop_process_by_pid_file "${pid_file}"
}

case "$SERVICE_COMMAND" in
start)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${MONGODB_CLUSTER_MODE}" != "none" ]; then
        # set the variables needed by mongodb-init
        . $MONGODB_HOME/conf/mongodb

        if [ "${MONGODB_CLUSTER_MODE}" == "sharding" ]; then
            # check and initialize the database if needed
            bash $BIN_DIR/mongodb-sharding-init.sh >${MONGODB_HOME}/logs/mongodb-init.log 2>&1
        else
            # check and initialize the database if needed
            bash $BIN_DIR/mongodb-init.sh >${MONGODB_HOME}/logs/mongodb-init.log 2>&1
        fi

        if [ "${MONGODB_CLUSTER_MODE}" == "sharding" ]; then
            if [ "${MONGODB_SHARDING_CLUSTER_ROLE}" == "configsvr" ]; then
                # start config server before mongos
                start_mongod
                start_mongos
            elif [ "${MONGODB_SHARDING_CLUSTER_ROLE}" == "shardsvr" ]; then
                # start mongos before shard
                start_mongos
                start_mongod
            else
                start_mongos
            fi
        else
            start_mongod
        fi
    fi
    ;;
stop)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${MONGODB_CLUSTER_MODE}" != "none" ]; then
        if [ "${MONGODB_CLUSTER_MODE}" == "sharding" ]; then
            if [ "${MONGODB_SHARDING_CLUSTER_ROLE}" == "configsvr" ]; then
                # start config server before mongos
                stop_mongos
                stop_mongod
            elif [ "${MONGODB_SHARDING_CLUSTER_ROLE}" == "shardsvr" ]; then
                # start mongos before shard
                stop_mongod
                stop_mongos
            else
                stop_mongos
            fi
        else
            stop_mongod
        fi
    fi
    ;;
-h|--help)
    echo "Usage: $0 start|stop --head" >&2
    ;;
*)
    echo "Usage: $0 start|stop --head" >&2
    ;;
esac

exit 0
