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

if [ ! -n "${FLINK_HOME}" ]; then
    echo "Flink is not installed."
    exit 1
fi

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime

set_head_option "$@"
set_service_command "$@"

case "$SERVICE_COMMAND" in
start)
    if [ "$IS_HEAD_NODE" == "true" ]; then
        # Create dirs on cloud storage if needed
        # This needs to be done after hadoop file system has been configured correctly
        ${HADOOP_HOME}/bin/hadoop --loglevel WARN fs -mkdir -p /shared/flink-checkpoints
        ${HADOOP_HOME}/bin/hadoop --loglevel WARN fs -mkdir -p /shared/flink-savepoints

        echo "Starting Flink History Server..."
        # Make sure HADOOP_CLASSPATH is set
        export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
        $FLINK_HOME/bin/historyserver.sh start > /dev/null
        echo "Starting Jupyter..."
        nohup jupyter lab --no-browser > $RUNTIME_PATH/jupyter/logs/jupyterlab.log 2>&1 &
    fi
    ;;
stop)
    if [ "$IS_HEAD_NODE" == "true" ]; then
        # Make sure HADOOP_CLASSPATH is set
        export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
        $FLINK_HOME/bin/historyserver.sh stop > /dev/null
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
