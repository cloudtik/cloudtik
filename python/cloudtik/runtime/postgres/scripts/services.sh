#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

# import util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

# repmgr functions
. "$BIN_DIR"/repmgr.sh

set_head_option "$@"
set_service_command "$@"
set_node_address
set_head_address

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
POSTGRES_HOME=$RUNTIME_PATH/postgres

start_repmgrd() {
  repmgrd \
    -f $POSTGRES_REPMGR_CONF_FILE \
    --daemonize >${POSTGRES_HOME}/logs/repmgrd-start.log 2>&1
  is_regmgrd_running
}

get_seed_nodes() {
  if [ "${IS_HEAD_NODE}" == "true" ]; then
      local running_worker_hosts=$(cloudtik head worker-hosts --runtime=postgres --node-status=up-to-date)
      echo "${running_worker_hosts}"
  else
      # TODO: use service discovery or cluster live nodes to get more nodes
      echo "${HEAD_HOST_ADDRESS}"
  fi
}

case "$SERVICE_COMMAND" in
start)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${POSTGRES_CLUSTER_MODE}" != "none" ]; then
        # set the variables needed by postgres-init
        . $POSTGRES_HOME/conf/postgres

        if [ "${POSTGRES_REPMGR_ENABLED}" == "true" ]; then
            POSTGRES_REPMGR_SEED_NODES=$(get_seed_nodes)
            repmgr_set_role
        fi

        # check and initialize the database if needed
        # make sure this should work for run multiple times
        bash $BIN_DIR/postgres-init.sh postgres \
            -c config_file=${POSTGRES_CONF_FILE} >${POSTGRES_HOME}/logs/postgres-init.log 2>&1

        if [ "${POSTGRES_ARCHIVE_MODE}" == "true" ]; then
            # create dir here for mount runtime ready
            ARCHIVE_DIR="/cloudtik/fs/postgres/archives/${CLOUDTIK_CLUSTER}"
            mkdir -p "${ARCHIVE_DIR}"
        fi

        nohup postgres \
            -c config_file=${POSTGRES_CONF_FILE} \
            >${POSTGRES_HOME}/logs/postgres.log 2>&1 &

        if [ "${POSTGRES_REPMGR_ENABLED}" == "true" ]; then
            # wait for postgres service running
            wait_for_port "${POSTGRES_SERVICE_PORT}" "${NODE_IP_ADDRESS}"
            bash $BIN_DIR/repmgr-init.sh >${POSTGRES_HOME}/logs/repmgr-init.log 2>&1

            # wait until the repmgrd started.
            # The standby node record may have some delay
            if ! retry_while "start_repmgrd" "5"; then
                echo "Postgres repmgrd did not start"
            fi
        fi
    fi
    ;;
stop)
    if [ "${IS_HEAD_NODE}" == "true" ] \
        || [ "${POSTGRES_CLUSTER_MODE}" != "none" ]; then

        if [ "${POSTGRES_REPMGR_ENABLED}" == "true" ]; then
            POSTGRES_REPMGR_PID_FILE=${POSTGRES_HOME}/repmgrd.pid
            stop_process_by_pid_file "${POSTGRES_REPMGR_PID_FILE}"
        fi

        POSTGRES_PID_FILE=${POSTGRES_HOME}/postgres.pid
        stop_process_by_pid_file "${POSTGRES_PID_FILE}" "2"
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
