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
ELASTICSEARCH_HOME=$RUNTIME_PATH/elasticsearch

case "$SERVICE_COMMAND" in
start)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${ELASTICSEARCH_CLUSTER_MODE}" != "none" ]; then
        # set the variables needed by elasticsearch-init
        . $ELASTICSEARCH_HOME/config/elasticsearch

        # check and initialize elasticsearch if needed
        bash $BIN_DIR/elasticsearch-init.sh >${ELASTICSEARCH_HOME}/logs/elasticsearch-init.log 2>&1

        ELASTICSEARCH_PID_FILE=${ELASTICSEARCH_HOME}/elasticsearch.pid
        ${ELASTICSEARCH_HOME}/bin/elasticsearch \
            --pidfile ${ELASTICSEARCH_PID_FILE} \
            --daemonize >${ELASTICSEARCH_HOME}/logs/elasticsearch-start.log 2>&1
    fi
    ;;
stop)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${ELASTICSEARCH_CLUSTER_MODE}" != "none" ]; then
        ELASTICSEARCH_PID_FILE=${ELASTICSEARCH_HOME}/elasticsearch.pid
        stop_process_by_pid_file "${ELASTICSEARCH_PID_FILE}"
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
