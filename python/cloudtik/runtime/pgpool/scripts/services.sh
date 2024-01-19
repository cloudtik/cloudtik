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

PGPOOL_HOME=$(get_runtime_home pgpool)

case "$SERVICE_COMMAND" in
start)
    if [ "${PGPOOL_HIGH_AVAILABILITY}" == "true" ] \
        || [ "${IS_HEAD_NODE}" == "true" ]; then
        # source to get the variables needed
        . ${PGPOOL_HOME}/conf/pgpool
        pgpool \
          --config-file=${PGPOOL_CONF_FILE} \
          --pcp-file=${PGPOOL_PCP_FILE} \
          --hba-file=${PGPOOL_HBA_FILE} \
          --discard-status >${PGPOOL_HOME}/logs/pgpool-start.log 2>&1
    fi
    ;;
stop)
    if [ "${PGPOOL_HIGH_AVAILABILITY}" == "true" ] \
        || [ "${IS_HEAD_NODE}" == "true" ]; then
        # source to get the variables needed
        . ${PGPOOL_HOME}/conf/pgpool
        PID_FILE=${PGPOOL_HOME}/run/pgpool.pid
        if [[ -f "$PID_FILE" ]]; then
            pgpool \
              --config-file=${PGPOOL_CONF_FILE} \
              --pcp-file=${PGPOOL_PCP_FILE} \
              --hba-file=${PGPOOL_HBA_FILE} \
              -m fast stop
        fi
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
