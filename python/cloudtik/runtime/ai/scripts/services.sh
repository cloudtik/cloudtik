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

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
MLFLOW_HOME=$RUNTIME_PATH/mlflow

set_head_option "$@"
set_service_command "$@"

case "$SERVICE_COMMAND" in
start)
    if [ "${MLFLOW_HIGH_AVAILABILITY}" == "true" ] \
      || [ "${IS_HEAD_NODE}" == "true" ]; then
        set_head_address

        # Will set BACKEND_STORE_URI and DEFAULT_ARTIFACT_ROOT
        . $MLFLOW_HOME/conf/mlflow

        if [ "${IS_HEAD_NODE}" == "true" ]; then
            # do schema check and init only on head
            init_schema
        fi

        # Start MLflow service
        nohup mlflow server \
            --backend-store-uri ${BACKEND_STORE_URI} \
            --default-artifact-root ${DEFAULT_ARTIFACT_ROOT} \
            --host 0.0.0.0 -p 5001 >${MLFLOW_HOME}/logs/mlflow.log 2>&1 &
    fi
    ;;
stop)
    if [ "${MLFLOW_HIGH_AVAILABILITY}" == "true" ] \
      || [ "${IS_HEAD_NODE}" == "true" ]; then
        # Stop MLflow service
        stop_process_by_command "mlflow.server:app"
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
