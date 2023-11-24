#!/bin/bash

# Current bin directory
BIN_DIR=`dirname "$0"`
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

# import util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

set_head_option "$@"
set_service_command "$@"
set_node_ip_address

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
MINIO_HOME=$RUNTIME_PATH/minio

case "$SERVICE_COMMAND" in
start)
    # only start services for the seq id that in valid range
    if [ "$CLOUDTIK_RUNTIME_ENV_NODE_SEQ_ID" -le "$MINIO_CLUSTER_SIZE" ]; then
        # Will set MINIO_OPTS and MINIO_VOLUMES
        . $MINIO_HOME/conf/minio
        nohup ${MINIO_HOME}/minio \
              server $MINIO_OPTS $MINIO_VOLUMES >${MINIO_HOME}/logs/minio.log 2>&1 &
    fi
    ;;
stop)
    # only start services for the seq id that in valid range
    if [ "$CLOUDTIK_RUNTIME_ENV_NODE_SEQ_ID" -le "$MINIO_CLUSTER_SIZE" ]; then
        stop_process_by_name "minio"
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
