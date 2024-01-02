#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

# import util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

# schema initialization functions
. "$BIN_DIR"/schema-init.sh

if [ ! -n "${METASTORE_HOME}" ]; then
    echo "Hive Metastore is not installed."
    exit 1
fi

set_head_option "$@"
set_service_command "$@"

case "$SERVICE_COMMAND" in
start)
    if [ "${METASTORE_HIGH_AVAILABILITY}" == "true" ] \
      || [ "${IS_HEAD_NODE}" == "true" ]; then
        if [ "${IS_HEAD_NODE}" == "true" ]; then
            # do schema check and init only on head
            init_schema
        fi

        nohup $METASTORE_HOME/bin/start-metastore \
          >${METASTORE_HOME}/logs/start-metastore.log 2>&1 &
    fi
    ;;
stop)
    if [ "${METASTORE_HIGH_AVAILABILITY}" == "true" ] \
      || [ "${IS_HEAD_NODE}" == "true" ]; then
        stop_process_by_command "HiveMetaStore"
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
