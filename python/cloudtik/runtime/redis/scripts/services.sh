#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

# import util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

# redis sentinel functions
. "$BIN_DIR"/redis-sentinel.sh

set_head_option "$@"
set_service_command "$@"
set_node_address
set_head_address

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
REDIS_HOME=$RUNTIME_PATH/redis

get_seed_nodes() {
  if [ "${IS_HEAD_NODE}" == "true" ]; then
      local running_worker_hosts=$(cloudtik head worker-hosts --runtime=redis --node-status=up-to-date)
      echo "${running_worker_hosts}"
  else
      # TODO: use service discovery or cluster live nodes to get more nodes
      echo "${HEAD_HOST_ADDRESS}"
  fi
}

case "$SERVICE_COMMAND" in
start)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${REDIS_CLUSTER_MODE}" != "none" ]; then
        # set the variables needed by redis-init
        . $REDIS_HOME/etc/redis

        if [ "${REDIS_SENTINEL_ENABLED}" == "true" ]; then
            REDIS_SENTINEL_SEED_NODES=$(get_seed_nodes)
            redis_sentinel_set_role
        fi

        # check and initialize redis if needed
        bash $BIN_DIR/redis-init.sh >${REDIS_HOME}/logs/redis-init.log 2>&1

        REDIS_PID_FILE=${REDIS_HOME}/redis-server.pid
        redis-server \
            ${REDIS_CONF_FILE} \
            --pidfile ${REDIS_PID_FILE} \
            --daemonize yes >${REDIS_HOME}/logs/redis-server-start.log 2>&1

        if [ "${REDIS_SENTINEL_ENABLED}" == "true" ]; then
            # wait for redis service running
            wait_for_port "${REDIS_SERVICE_PORT}" "${NODE_IP_ADDRESS}"
            bash $BIN_DIR/redis-sentinel-init.sh >${REDIS_HOME}/logs/redis-sentinel-init.log 2>&1

            REDIS_SENTINEL_PID_FILE=${REDIS_HOME}/redis-sentinel.pid
            redis-server \
                ${REDIS_SENTINEL_CONF_FILE} \
                --pidfile ${REDIS_SENTINEL_PID_FILE} \
                --daemonize yes --sentinel >${REDIS_HOME}/logs/redis-sentinel-start.log 2>&1
        fi
    fi
    ;;
stop)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${REDIS_CLUSTER_MODE}" != "none" ]; then

        if [ "${REDIS_SENTINEL_ENABLED}" == "true" ]; then
            REDIS_SENTINEL_PID_FILE=${REDIS_HOME}/redis-sentinel.pid
            stop_process_by_pid_file "${REDIS_SENTINEL_PID_FILE}"
        fi

        REDIS_PID_FILE=${REDIS_HOME}/redis-server.pid
        stop_process_by_pid_file "${REDIS_PID_FILE}"
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
