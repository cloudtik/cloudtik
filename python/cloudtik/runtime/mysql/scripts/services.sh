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
MYSQL_HOME=$RUNTIME_PATH/mysql

case "$SERVICE_COMMAND" in
start)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${MYSQL_CLUSTER_MODE}" != "none" ]; then
        MYSQL_CONFIG_FILE=${MYSQL_HOME}/conf/my.cnf
        nohup mysqld \
            --defaults-file=${MYSQL_CONFIG_FILE} \
            >${MYSQL_HOME}/logs/mysqld.log 2>&1 &

        if [ "${IS_HEAD_NODE}" == "true" ] \
            && [ "${MYSQL_CLUSTER_MODE}" == "group_replication" ]; then
            # Case 1 and Case 2. start group replication with bootstrap
            # TODO: distinguish for Case 3
            echo "Starting group replication"
            bash $BIN_DIR/start-group-replication.sh \
              -h ${HEAD_IP_ADDRESS} >${MYSQL_HOME}/logs/mysql-group-replication.log 2>&1
        fi
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
