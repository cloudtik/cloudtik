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

case "$SERVICE_COMMAND" in
start)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${MONGODB_CLUSTER_MODE}" != "none" ]; then
        MONGODB_CONFIG_FILE=${MONGODB_HOME}/conf/mongod.conf
        mongod \
            --config ${MONGODB_CONFIG_FILE} \
            >${MONGODB_HOME}/logs/mongod-start.log 2>&1
    fi
    ;;
stop)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${MONGODB_CLUSTER_MODE}" != "none" ]; then
        MONGODB_PID_FILE=${MONGODB_HOME}/mongodb.pid
        stop_process_by_pid_file "${MONGODB_PID_FILE}"
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
