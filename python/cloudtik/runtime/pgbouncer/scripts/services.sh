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

PGBOUNCER_HOME=$(get_runtime_home pgbouncer)

case "$SERVICE_COMMAND" in
start)
    if [ "${PGBOUNCER_HIGH_AVAILABILITY}" == "true" ] \
        || [ "${IS_HEAD_NODE}" == "true" ]; then
        # source to get the variables needed
        . ${PGBOUNCER_HOME}/conf/pgbouncer
        pgbouncer \
          -d ${PGBOUNCER_CONF_FILE} \
           >${PGBOUNCER_HOME}/logs/pgbouncer-start.log 2>&1
    fi
    ;;
stop)
    if [ "${PGBOUNCER_HIGH_AVAILABILITY}" == "true" ] \
        || [ "${IS_HEAD_NODE}" == "true" ]; then
        # source to get the variables needed
        . ${PGBOUNCER_HOME}/conf/pgbouncer
        stop_process_by_pid_file "${PGBOUNCER_PID_FILE}"
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
