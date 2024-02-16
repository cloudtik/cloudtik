#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
DNSMASQ_HOME=$RUNTIME_PATH/dnsmasq
DNSMASQ_BACKUP_RESOLV_CONF=${DNSMASQ_HOME}/conf/resolv.conf.backup

# import util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

set_head_option "$@"
set_service_command "$@"

case "$SERVICE_COMMAND" in
start)
    sudo service dnsmasq start
    wait_for_port "${DNSMASQ_SERVICE_PORT}"

    if [ "${DNSMASQ_DEFAULT_RESOLVER}" == "true" ]; then
        if is_systemd_resolved_active; then
            update_systemd_resolved "dnsmasq" "127.0.0.1"
        else
            # update the /etc/resolv.conf
            update_resolv_conf ${DNSMASQ_BACKUP_RESOLV_CONF} "127.0.0.1"
        fi
    fi
    ;;
stop)
    if [ "${DNSMASQ_DEFAULT_RESOLVER}" == "true" ]; then
        if is_systemd_resolved_active; then
            restore_systemd_resolved "dnsmasq"
        else
            # restore the /etc/resolv.conf
            restore_resolv_conf ${DNSMASQ_BACKUP_RESOLV_CONF}
        fi
    fi

    sudo service dnsmasq stop
    ;;
-h|--help)
    echo "Usage: $0 start|stop --head" >&2
    ;;
*)
    echo "Usage: $0 start|stop --head" >&2
    ;;
esac

exit 0
