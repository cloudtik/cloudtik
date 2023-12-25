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

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
ETCD_HOME=$RUNTIME_PATH/etcd

case "$SERVICE_COMMAND" in
start)
    if [ "$IS_HEAD_NODE" == "false" ]; then
        # etcd run only on workers
        ETCD_CONFIG_FILE=${ETCD_HOME}/conf/etcd.yaml
        nohup etcd --config-file=${ETCD_CONFIG_FILE} >/dev/null 2>&1 &
        wait_for_port "${ETCD_SERVICE_PORT}"
    fi
    ;;
stop)
    if [ "$IS_HEAD_NODE" == "false" ]; then
        # etcd run only on workers
        stop_process_by_name "etcd"
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
