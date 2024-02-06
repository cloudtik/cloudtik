#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

# import util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

if [ ! -n "${HADOOP_HOME}" ]; then
    echo "HADOOP_HOME environment variable is not set."
    exit 1
fi

set_head_option "$@"
set_service_command "$@"

# HDFS use its own conf dir
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hdfs
HDFS_DAEMON_CMD="$HADOOP_HOME/bin/hdfs --daemon"

case "$SERVICE_COMMAND" in
start)
    # check and initialize if needed
    . ${HADOOP_CONF_DIR}/hdfs
    bash $BIN_DIR/hdfs-init.sh >${HADOOP_HOME}/logs/hdfs-init.log 2>&1

    if [ "${HDFS_CLUSTER_MODE}" == "ha_cluster" ]; then
        if [ "${HDFS_CLUSTER_ROLE}" == "name" ]; then
            $HDFS_DAEMON_CMD start namenode
            if [ "${HDFS_AUTO_FAILOVER}" == "true" ]; then
                $HDFS_DAEMON_CMD start zkfc
            fi
        elif [ "${HDFS_CLUSTER_ROLE}" == "journal" ]; then
            $HDFS_DAEMON_CMD start journalnode
        else
            $HDFS_DAEMON_CMD start datanode
        fi
    else
        if [ "$IS_HEAD_NODE" == "true" ]; then
            $HDFS_DAEMON_CMD start namenode
        else
            $HDFS_DAEMON_CMD start datanode
        fi
    fi
    ;;
stop)
    if [ "${HDFS_CLUSTER_MODE}" == "ha_cluster" ]; then
        if [ "${HDFS_CLUSTER_ROLE}" == "name" ]; then
            if [ "${HDFS_AUTO_FAILOVER}" == "true" ]; then
                $HDFS_DAEMON_CMD stop zkfc
            fi
            $HDFS_DAEMON_CMD stop namenode
        elif [ "${HDFS_CLUSTER_ROLE}" == "journal" ]; then
            $HDFS_DAEMON_CMD stop journalnode
        else
            $HDFS_DAEMON_CMD stop datanode
        fi
    else
        if [ "$IS_HEAD_NODE" == "true" ]; then
            $HDFS_DAEMON_CMD stop namenode
        else
            $HDFS_DAEMON_CMD stop datanode
        fi
    fi
    ;;
-h|--help)
    echo "Usage: $0 start|stop [--head]" >&2
    ;;
*)
    echo "Usage: $0 start|stop [--head]" >&2
    ;;
esac

exit 0
