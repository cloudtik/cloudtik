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

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
POSTGRES_HOME=$RUNTIME_PATH/postgres

case "$SERVICE_COMMAND" in
start)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${POSTGRES_CLUSTER_MODE}" != "none" ]; then
        if [ "${POSTGRES_ARCHIVE_MODE}" == "true" ]; then
            # create dir here for mount runtime ready
            ARCHIVE_DIR="/cloudtik/fs/postgres/archives/${CLOUDTIK_CLUSTER}"
            mkdir -p "${ARCHIVE_DIR}"
        fi
        POSTGRES_CONFIG_FILE=${POSTGRES_HOME}/conf/postgresql.conf
        nohup postgres \
            -c config_file=${POSTGRES_CONFIG_FILE} \
            >${POSTGRES_HOME}/logs/postgres.log 2>&1 &
    fi
    ;;
stop)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${POSTGRES_CLUSTER_MODE}" != "none" ]; then
        POSTGRES_PID_FILE=${POSTGRES_HOME}/postgres.pid
        stop_process_by_pid_file "${POSTGRES_PID_FILE}"
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
