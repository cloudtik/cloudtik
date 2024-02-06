#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

# import util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

if [ ! -n "${HADOOP_HOME}" ]; then
    echo "Hadoop is not installed."
    exit 1
fi

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

set_head_option "$@"
set_service_command "$@"
set_head_address

case "$SERVICE_COMMAND" in
start)
    if [ "$IS_HEAD_NODE" == "true" ]; then
        echo "Starting Resource Manager..."
        $HADOOP_HOME/bin/yarn --daemon start resourcemanager
    else
        $HADOOP_HOME/bin/yarn --daemon start nodemanager
    fi
    ;;
stop)
    if [ "$IS_HEAD_NODE" == "true" ]; then
        $HADOOP_HOME/bin/yarn --daemon stop resourcemanager
    else
        $HADOOP_HOME/bin/yarn --daemon stop nodemanager
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
