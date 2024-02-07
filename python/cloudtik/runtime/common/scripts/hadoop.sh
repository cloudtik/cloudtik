#!/bin/bash

########################
# Get the HDFS name node ids
# Globals:
#   HDFS_NUM_NAME_NODES
# arguments:
#   None
# returns:
#   The string list of name node ids
#########################
get_hdfs_name_nodes() {
    local name_nodes=""
    local end_name_id=${HDFS_NUM_NAME_NODES}
    for i in $(seq 1 $end_name_id); do
        if [ -z "${name_nodes}" ]; then
            name_nodes="nn$i"
        else
            name_nodes="${name_nodes},nn$i"
        fi
    done
    echo "${name_nodes}"
}

########################
# Get name node addresses of a single name node given its index
# Globals:
#   HDFS_NAME_SERVICE, HDFS_SERVICE_PORT, HDFS_HTTP_PORT
# arguments:
#   the index
# returns:
#   The properties of Hadoop configuration
#########################
get_hdfs_name_node_addresses() {
    local name_index="$1"
    local name_service="${HDFS_NAME_SERVICE}"
    local name_cluster="${HDFS_NAME_CLUSTER:-"${name_service}"}"
    local rpc_port=${HDFS_SERVICE_PORT:-9000}
    local http_port=${HDFS_HTTP_PORT:-9870}
    local address_properties="\
    <property>\n\
        <name>dfs.namenode.rpc-address.${name_service}.nn${name_index}</name>\n\
        <value>${name_cluster}-${name_index}.node.cloudtik:${rpc_port}</value>\n\
    </property>\n\
    <property>\n\
        <name>dfs.namenode.http-address.${name_service}.nn${name_index}</name>\n\
        <value>${name_cluster}-${name_index}.node.cloudtik:${http_port}</value>\n\
    </property>"
    echo "${address_properties}"
}

get_hdfs_name_addresses() {
    local name_addresses=""
    local end_name_id=${HDFS_NUM_NAME_NODES}
    for i in $(seq 1 $end_name_id); do
        local name_node_addresses="$(get_hdfs_name_node_addresses "$i")"
        if [ -z "${name_addresses}" ]; then
            name_addresses="${name_node_addresses}"
        else
            name_addresses="${name_addresses}\n${name_node_addresses}"
        fi
    done
    echo "${name_addresses}"
}
