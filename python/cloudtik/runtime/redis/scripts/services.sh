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
REDIS_HOME=$RUNTIME_PATH/redis

case "$SERVICE_COMMAND" in
start)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${REDIS_CLUSTER_MODE}" != "none" ]; then
        # set the variables needed by postgres-init
        . $REDIS_HOME/etc/redis

        # check and initialize redis if needed
        bash $BIN_DIR/redis-init.sh >${REDIS_HOME}/logs/redis-init.log 2>&1

        REDIS_PID_FILE=${REDIS_HOME}/redis-server.pid
        redis-server \
            ${REDIS_CONF_FILE} \
            --pidfile ${REDIS_PID_FILE} \
            --daemonize yes >${REDIS_HOME}/logs/redis-server-start.log 2>&1
    fi
    ;;
stop)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${REDIS_CLUSTER_MODE}" != "none" ]; then
        REDIS_PID_FILE=${REDIS_HOME}/redis-server.pid
        stop_process_by_pid_file "${REDIS_PID_FILE}"
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
