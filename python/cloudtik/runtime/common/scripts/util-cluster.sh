#!/bin/bash

set_node_address() {
    set_node_ip_address
    # set the node host address when applicable
    set_node_host_address
}

set_node_ip_address() {
    if [ -z "${NODE_IP_ADDRESS}" ]; then
        NODE_IP_ADDRESS=$(get_node_ip_address)
    fi
}

set_node_host_address() {
    if [ -z "${NODE_HOST_ADDRESS}" ]; then
        NODE_HOST_ADDRESS=$(get_node_host_address)
    fi
}

########################
# Get the node ip address
# Arguments:
#   None
# Returns:
#   The node ip address
#########################
get_node_ip_address() {
    local node_ip_address
    if [ -z "${NODE_IP_ADDRESS}" ]; then
        if [ -n "${CLOUDTIK_NODE_IP}" ]; then
            node_ip_address=${CLOUDTIK_NODE_IP}
        else
            node_ip_address=$(hostname -I | awk '{print $1}')
        fi
    else
        node_ip_address=${NODE_IP_ADDRESS}
    fi
    echo "${node_ip_address}"
}

########################
# Get the node host address. If there is hostname, use hostname
# if there is no hostname, ip address will be used.
# Arguments:
#   None
# Returns:
#   The node host address
#########################
get_node_host_address() {
    local node_host_address
    if [ -z "${NODE_HOST_ADDRESS}" ]; then
        if [ -n "${CLOUDTIK_NODE_HOST}" ]; then
            node_host_address=${CLOUDTIK_NODE_HOST}
        else
            node_host_address=$(get_node_ip_address)
        fi
    else
        node_host_address=${NODE_HOST_ADDRESS}
    fi
    echo "${node_host_address}"
}

set_head_address() {
    set_head_ip_address
    # set the head host address when applicable
    set_head_host_address
}

set_head_ip_address() {
    if [ -z "${HEAD_IP_ADDRESS}" ]; then
        HEAD_IP_ADDRESS=$(get_head_ip_address)
    fi
}

set_head_host_address() {
    if [ -z "${HEAD_HOST_ADDRESS}" ]; then
        HEAD_HOST_ADDRESS=$(get_head_host_address)
    fi
}

get_head_ip_address() {
    local head_ip_address
    if [ -z "${HEAD_IP_ADDRESS}" ]; then
        if [ "$IS_HEAD_NODE" == "true" ]; then
            if [ -n "${CLOUDTIK_NODE_IP}" ]; then
                head_ip_address=${CLOUDTIK_NODE_IP}
            else
                head_ip_address=$(hostname -I | awk '{print $1}')
            fi
        else
            if [ -n "${CLOUDTIK_HEAD_IP}" ]; then
                head_ip_address=${CLOUDTIK_HEAD_IP}
            else
                echo "Error: CLOUDTIK_HEAD_IP environment variable should be set."
                exit 1
            fi
        fi
    else
        head_ip_address=${HEAD_IP_ADDRESS}
    fi
    echo "${head_ip_address}"
}

get_head_host_address() {
    local head_host_address
    if [ -z "${HEAD_HOST_ADDRESS}" ]; then
        if [ -n "${CLOUDTIK_HEAD_HOST}" ]; then
            head_host_address=${CLOUDTIK_HEAD_HOST}
        else
            if [ "$IS_HEAD_NODE" == "true" ]; then
                head_host_address=$(get_node_host_address)
            else
                head_host_address=$(get_head_ip_address)
            fi
        fi
    else
        head_host_address=${HEAD_HOST_ADDRESS}
    fi
    echo "${head_host_address}"
}

set_head_option() {
    # this function set the head variable based on the arguments processed by getopt
    IS_HEAD_NODE=false
    while true
    do
        case "$1" in
        -h|--head)
            IS_HEAD_NODE=true
            ;;
        --)
            shift
            break
            ;;
        esac
        shift
    done
}

set_service_command() {
    # this function set the SERVICE_COMMAND
    # based on the arguments processed by getopt
    while true
    do
        case "$1" in
        --)
            shift
            break
            ;;
        esac
        shift
    done
    SERVICE_COMMAND="$1"
}

get_data_disk_dirs() {
    local data_disk_dirs=""
    if [ -d "/mnt/cloudtik" ]; then
        for data_disk in /mnt/cloudtik/*; do
            [ -d "$data_disk" ] || continue
            if [ -z "$data_disk_dirs" ]; then
                data_disk_dirs=$data_disk
            else
                data_disk_dirs="$data_disk_dirs,$data_disk"
            fi
        done
    fi
    echo "${data_disk_dirs}"
}

get_first_data_disk_dir() {
    local data_disk_dir=""
    if [ -d "/mnt/cloudtik" ]; then
        for data_disk in /mnt/cloudtik/*; do
            [ -d "$data_disk" ] || continue
            data_disk_dir=$data_disk
            break
        done
    fi
    echo "${data_disk_dir}"
}

########################
# Get a comma separated list of data disk paths with named data dir
# Arguments:
#   $1 - sub_dir
#   $2 - make_dir: default false
# Returns:
#   Comma separated list of data disk paths
#########################
get_data_disk_dirs_of() {
    local sub_dir="${1:?Sub directory is required}"
    local make_dir=${2:-false}
    local data_disk_dirs=""
    if [ -d "/mnt/cloudtik" ]; then
        for data_disk in /mnt/cloudtik/*; do
            [ -d "$data_disk" ] || continue
            local data_dir="$data_disk/$sub_dir"
            if [[ $make_dir = true ]]; then
              mkdir -p "$data_dir"
            fi
            if [ -z "$data_disk_dirs" ]; then
                data_disk_dirs="$data_dir"
            else
                data_disk_dirs="$data_disk_dirs,$data_dir"
            fi
        done
    fi
    echo "${data_disk_dirs}"
}

########################
# Get the runtime home dir given the runtime name
# Arguments:
#   The runtime name
# Returns:
#   The runtime home dir
#########################
get_runtime_home() {
    local -r runtime_name="${1:?The runtime name is required}"
    local -r runtime_home=/home/$(whoami)/runtime/${runtime_name}
    echo "${runtime_home}"
}
