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
MINIO_HOME=$RUNTIME_PATH/minio

case "$SERVICE_COMMAND" in
start)
    if [ "${IS_HEAD_NODE}" != "true" ] \
        || [ "${MINIO_SERVICE_ON_HEAD}" != "false" ]; then
        # only start services for the seq id that in valid range
        if [ "$CLOUDTIK_NODE_SEQ_ID" -le "$MINIO_MAX_SEQ_ID" ]; then
            # Will set MINIO_OPTS and MINIO_VOLUMES
            . $MINIO_HOME/conf/minio
            nohup ${MINIO_HOME}/bin/minio \
                  server $MINIO_OPTS $MINIO_VOLUMES >${MINIO_HOME}/logs/minio.log 2>&1 &
        else
            echo "Warning: MinIO service on this node started because it is not in the valid pool size range."
        fi
    fi
    ;;
stop)
    if [ "${IS_HEAD_NODE}" != "true" ] \
        || [ "${MINIO_SERVICE_ON_HEAD}" != "false" ]; then
        # only start services for the seq id that in valid range
        if [ "$CLOUDTIK_NODE_SEQ_ID" -le "$MINIO_MAX_SEQ_ID" ]; then
            stop_process_by_name "minio"
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
