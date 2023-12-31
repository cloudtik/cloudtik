#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
KONG_HOME=$RUNTIME_PATH/kong
KONG_CONFIG_FILE=${KONG_HOME}/conf/kong.conf

# import util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

# schema initialization functions
. "$BIN_DIR"/schema-init.sh

set_head_option "$@"
set_service_command "$@"

case "$SERVICE_COMMAND" in
start)
    if [ "${IS_HEAD_NODE}" == "true" ]; then
        # do schema check and init only on head
        init_schema
    fi
    if [ "${KONG_HIGH_AVAILABILITY}" == "true" ] \
        || [ "${IS_HEAD_NODE}" == "true" ]; then
        sudo env "PATH=$PATH" kong start \
          -c ${KONG_CONFIG_FILE} \
          >${KONG_HOME}/logs/kong.log 2>&1
        wait_for_port "${KONG_SERVICE_PORT}"
    fi
    ;;
stop)
    if [ "${KONG_HIGH_AVAILABILITY}" == "true" ] \
        || [ "${IS_HEAD_NODE}" == "true" ]; then
        sudo env "PATH=$PATH" kong stop \
          >${KONG_HOME}/logs/kong.log 2>&1
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
