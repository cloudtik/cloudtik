#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

# import util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

if ! command -v consul &> /dev/null
then
    echo "Consul is not installed."
    exit 1
fi

set_head_option "$@"
set_service_command "$@"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
CONSUL_HOME=$RUNTIME_PATH/consul
CONSUL_PID_FILE=${CONSUL_HOME}/consul-agent.pid

case "$SERVICE_COMMAND" in
start)
    CONSUL_CONFIG_DIR=${CONSUL_HOME}/consul.d
    CONSUL_LOG_FILE=${CONSUL_HOME}/logs/consul-agent.log
    # Run server or client agent on each node
    nohup consul agent \
        -config-dir=${CONSUL_CONFIG_DIR} \
        -log-file=${CONSUL_LOG_FILE} \
        -pid-file=${CONSUL_PID_FILE} >${CONSUL_HOME}/logs/consul-agent-start.log 2>&1 &
    wait_for_port "${CONSUL_CLIENT_PORT}"
    ;;
stop)
    # Stop server or client agent
    stop_process_by_pid_file "${CONSUL_PID_FILE}"
    ;;
-h|--help)
    echo "Usage: $0 start|stop --head" >&2
    ;;
*)
    echo "Usage: $0 start|stop --head" >&2
    ;;
esac

exit 0
