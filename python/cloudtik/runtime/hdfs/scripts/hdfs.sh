#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

# Load Generic Libraries
. "$ROOT_DIR"/common/scripts/util-log.sh
. "$ROOT_DIR"/common/scripts/util-cluster.sh

get_first_dfs_dir() {
    local data_disk_dir=$(get_first_data_disk_dir)
    local data_dir
    if [ -z "$data_disk_dir" ]; then
        data_dir="${HADOOP_HOME}/data/dfs"
    else
        data_dir="$data_disk_dir/dfs"
    fi
    echo "${data_dir}"
}

initialize_name_node() {
    local init_method="${1:?init method is required}"
    local dfs_dir=$(get_first_dfs_dir)
    local hdfs_init_file=${dfs_dir}/.initialized
    if [ ! -f "${hdfs_init_file}" ]; then
        export HADOOP_CONF_DIR=${HDFS_CONF_DIR}

        # format ZK if needed
        if [ "${init_method}" == "format" ] \
            && [ "${HDFS_AUTO_FAILOVER}" == "true" ]; then
            info "Formatting ZooKeeper for high availability..."
            ${HADOOP_HOME}/bin/hdfs zkfc -formatZK -force

            info "Formatting HDFS NameNode..."
        else
            info "Bootstrapping Standby NameNode..."
        fi

        # Stop namenode in case it was running left from last try
        ${HADOOP_HOME}/bin/hdfs --daemon stop namenode > /dev/null 2>&1
        # Format hdfs once
        ${HADOOP_HOME}/bin/hdfs --loglevel WARN namenode -${init_method} -force

        if [ $? -eq 0 ]; then
            mkdir -p "${dfs_dir}"
            touch "${hdfs_init_file}"
        fi
    else
        info "This NameNode already initialized. Skip initialization."
    fi
}

format_hdfs() {
    # format only once
    initialize_name_node "format"
}

bootstrap_standby() {
    # bootstrap standby only once
    initialize_name_node "bootstrapStandby"
}

initialize_simple_hdfs() {
    if [ "$HDFS_HEAD_NODE" == "true" ]; then
        format_hdfs
    fi
}

initialize_name_cluster() {
    # for HA, the name node on head will do the format for the first time
    if [ "$HDFS_HEAD_NODE" == "true" ]; then
        format_hdfs
    else
        bootstrap_standby
    fi
}

initialize_ha_cluster() {
    if [ "${HDFS_CLUSTER_ROLE}" == "name" ]; then
        initialize_name_cluster
    fi
}

hdfs_initialize() {
    HDFS_CONF_DIR=${HADOOP_HOME}/etc/hdfs
    if [ "${HDFS_CLUSTER_MODE}" == "ha_cluster" ]; then
        initialize_ha_cluster
    else
        initialize_simple_hdfs
    fi
}
