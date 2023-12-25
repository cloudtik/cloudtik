#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
APISIX_HOME=$RUNTIME_PATH/apisix
APISIX_CONFIG_FILE=${APISIX_HOME}/conf/config.yaml

# import util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

set_head_option "$@"
set_service_command "$@"

case "$SERVICE_COMMAND" in
start)
    if [ "${APISIX_HIGH_AVAILABILITY}" == "true" ] \
        || [ "${IS_HEAD_NODE}" == "true" ]; then
        sudo apisix start \
          -c ${APISIX_CONFIG_FILE} \
          >${APISIX_HOME}/logs/apisix.log 2>&1
        wait_for_port "${APISIX_SERVICE_PORT}"
    fi
    ;;
stop)
    if [ "${APISIX_HIGH_AVAILABILITY}" == "true" ] \
        || [ "${IS_HEAD_NODE}" == "true" ]; then
        sudo apisix stop \
          >${APISIX_HOME}/logs/apisix.log 2>&1
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
