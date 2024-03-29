#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
COREDNS_HOME=$RUNTIME_PATH/coredns
COREDNS_CONFIG_FILE=${COREDNS_HOME}/conf/Corefile
COREDNS_PID_FILE=${COREDNS_HOME}/coredns.pid
COREDNS_BACKUP_RESOLV_CONF=${COREDNS_HOME}/conf/resolv.conf.backup

# import util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

set_head_option "$@"
set_service_command "$@"

case "$SERVICE_COMMAND" in
start)
    sudo nohup ${COREDNS_HOME}/coredns \
      -conf ${COREDNS_CONFIG_FILE} \
      -pidfile ${COREDNS_PID_FILE} \
      >${COREDNS_HOME}/logs/coredns.log 2>&1 &
    wait_for_port "${COREDNS_SERVICE_PORT}"

    if [ "${COREDNS_DEFAULT_RESOLVER}" == "true" ]; then
        if is_systemd_resolved_active; then
            update_systemd_resolved "coredns" "127.0.0.1"
        else
            # update the /etc/resolv.conf
            update_resolv_conf ${COREDNS_BACKUP_RESOLV_CONF} "127.0.0.1"
        fi
    fi
    ;;
stop)
    if [ "${COREDNS_DEFAULT_RESOLVER}" == "true" ]; then
        if is_systemd_resolved_active; then
            restore_systemd_resolved "coredns"
        else
            # restore the /etc/resolv.conf
            restore_resolv_conf ${COREDNS_BACKUP_RESOLV_CONF}
        fi
    fi
    stop_process_by_pid_file "${COREDNS_PID_FILE}"
    ;;
-h|--help)
    echo "Usage: $0 start|stop --head" >&2
    ;;
*)
    echo "Usage: $0 start|stop --head" >&2
    ;;
esac

exit 0
