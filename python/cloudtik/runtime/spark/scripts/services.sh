#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

if [ ! -n "${HADOOP_HOME}" ]; then
    echo "Hadoop is not installed."
    exit 1
fi

if [ ! -n "${SPARK_HOME}" ]; then
    echo "Spark is not installed."
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
        # Create event log dir on cloud storage if needed
        # This needs to be done after hadoop file system has been configured correctly
        ${HADOOP_HOME}/bin/hadoop --loglevel WARN fs -mkdir -p /shared/spark-events

        echo "Starting Spark History Server..."
        export SPARK_LOCAL_IP=${CLOUDTIK_NODE_IP}; $SPARK_HOME/sbin/start-history-server.sh > /dev/null
        echo "Starting Jupyter..."
        nohup jupyter lab --no-browser > $RUNTIME_PATH/jupyter/logs/jupyterlab.log 2>&1 &
    fi
    ;;
stop)
    if [ "$IS_HEAD_NODE" == "true" ]; then
        $SPARK_HOME/sbin/stop-history-server.sh
        stop_process_by_name "jupyter"
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
