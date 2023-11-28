#!/bin/bash

# Current bin directory
BIN_DIR=`dirname "$0"`
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

# import util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

set_head_option "$@"
set_service_command "$@"
set_node_ip_address

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
MYSQL_HOME=$RUNTIME_PATH/mysql

case "$SERVICE_COMMAND" in
start)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${MYSQL_CLUSTER_MODE}" != "none" ]; then
        MYSQL_CONFIG_FILE=${MYSQL_HOME}/conf/my.cnf
        nohup mysqld \
            --defaults-file=${MYSQL_CONFIG_FILE} \
            >${MYSQL_HOME}/logs/mysqld.log 2>&1 &
    fi
    ;;
stop)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${MYSQL_CLUSTER_MODE}" != "none" ]; then
        MYSQL_PID_FILE=/var/run/mysqld/mysqld.pid
        stop_process_by_pid_file "${MYSQL_PID_FILE}"
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
