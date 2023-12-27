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
NODEX_HOME=$RUNTIME_PATH/nodex

get_nodex_port() {
    local service_port=9100
    if [ ! -z "${NODEX_SERVICE_PORT}" ]; then
        service_port=${NODEX_SERVICE_PORT}
    fi
    echo "${service_port}"
}

case "$SERVICE_COMMAND" in
start)
    NODEX_SERVICE_PORT=$(get_nodex_port)
    NODEX_ADDRESS="${NODE_IP_ADDRESS}:${NODEX_SERVICE_PORT}"
    nohup ${NODEX_HOME}/nodex \
          --web.listen-address=${NODEX_ADDRESS} >${NODEX_HOME}/logs/nodex.log 2>&1 &
    ;;
stop)
    stop_process_by_name "nodex"
    ;;
-h|--help)
    echo "Usage: $0 start|stop --head" >&2
    ;;
*)
    echo "Usage: $0 start|stop --head" >&2
    ;;
esac

exit 0
