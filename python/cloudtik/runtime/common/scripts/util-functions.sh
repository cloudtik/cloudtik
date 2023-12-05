#!/bin/bash

# global variables
CLOUDTIK_DOWNLOADS="https://d30257nes7d4fq.cloudfront.net/downloads"

function set_head_address() {
    if [ -z "${HEAD_ADDRESS}" ]; then
        if [ $IS_HEAD_NODE == "true" ]; then
            if [ ! -n "${CLOUDTIK_NODE_IP}" ]; then
                HEAD_ADDRESS=$(hostname -I | awk '{print $1}')
            else
                HEAD_ADDRESS=${CLOUDTIK_NODE_IP}
            fi
        else
            if [ ! -n "${CLOUDTIK_HEAD_IP}" ]; then
                echo "Error: CLOUDTIK_HEAD_IP environment variable should be set."
                exit 1
            else
                HEAD_ADDRESS=${CLOUDTIK_HEAD_IP}
            fi
        fi
    fi
}

function set_node_ip_address() {
    if [ -z "${NODE_IP_ADDRESS}" ]; then
        if [ ! -n "${CLOUDTIK_NODE_IP}" ]; then
            NODE_IP_ADDRESS=$(hostname -I | awk '{print $1}')
        else
            NODE_IP_ADDRESS=${CLOUDTIK_NODE_IP}
        fi
    fi
}

function set_head_option() {
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

function set_service_command() {
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

function clean_install_cache() {
    (sudo rm -rf /var/lib/apt/lists/* \
        && sudo apt-get clean \
        && which conda > /dev/null && conda clean -itqy)
}

function get_data_disk_dirs() {
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

function get_first_data_disk_dir() {
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

function get_deb_arch() {
    local deb_arch="amd64"
    arch=$(uname -m)
    if [ "${arch}" == "aarch64" ]; then
        deb_arch="arm64"
    fi
    echo "${deb_arch}"
}

function stop_process_by_name() {
    local PROCESS_NAME=$1
    local MY_PID=$(pgrep ${PROCESS_NAME})
    if [ -n "${MY_PID}" ]; then
        echo "Stopping ${PROCESS_NAME}..."
        # SIGTERM = 15
        sudo kill -15 ${MY_PID} >/dev/null 2>&1
    fi
}

function stop_process_by_pid_file() {
    local PROCESS_PID_FILE=$1
    if sudo test -f "$PROCESS_PID_FILE"; then
        local PROCESS_NAME=$(basename "$PROCESS_PID_FILE")
        local MY_PID=$(sudo pgrep --pidfile ${PROCESS_PID_FILE})
        if [ -n "${MY_PID}" ]; then
            echo "Stopping ${PROCESS_NAME}..."
            # SIGTERM = 15
            sudo kill -15 ${MY_PID} >/dev/null 2>&1
        fi
    fi
}

function update_resolv_conf() {
    local BACKUP_RESOLV_CONF=$1
    cp /etc/resolv.conf ${BACKUP_RESOLV_CONF}
    shift
    SCRIPTS_DIR=$(dirname ${BASH_SOURCE[0]})
    sudo env PATH=$PATH python ${SCRIPTS_DIR}/resolv-conf.py "$@"
}

function restore_resolv_conf() {
    local BACKUP_RESOLV_CONF=$1
    if [ -f "${BACKUP_RESOLV_CONF}" ]; then
        sudo cp ${BACKUP_RESOLV_CONF} /etc/resolv.conf
    fi
}

########################
# Read the provided pid file and returns a PID
# Arguments:
#   $1 - Pid file
# Returns:
#   PID
#########################
get_pid_from_file() {
    local pid_file="${1:?pid file is missing}"

    if [[ -f "$pid_file" ]]; then
        if [[ -n "$(< "$pid_file")" ]] && [[ "$(< "$pid_file")" -gt 0 ]]; then
            echo "$(< "$pid_file")"
        fi
    fi
}

########################
# Check if a provided PID corresponds to a running service
# Arguments:
#   $1 - PID
# Returns:
#   Boolean
#########################
is_service_running() {
    local pid="${1:?pid is missing}"

    kill -0 "$pid" 2>/dev/null
}

########################
# Stop a service by sending a termination signal to its pid
# Arguments:
#   $1 - Pid file
#   $2 - Signal number (optional)
# Returns:
#   None
#########################
stop_service_using_pid() {
    local pid_file="${1:?pid file is missing}"
    local signal="${2:-}"
    local pid

    pid="$(get_pid_from_file "$pid_file")"
    [[ -z "$pid" ]] || ! is_service_running "$pid" && return

    if [[ -n "$signal" ]]; then
        kill "-${signal}" "$pid"
    else
        kill "$pid"
    fi

    local counter=10
    while [[ "$counter" -ne 0 ]] && is_service_running "$pid"; do
        sleep 1
        counter=$((counter - 1))
    done
}

########################
# Replace a regex-matching string in a file
# Arguments:
#   $1 - filename
#   $2 - match regex
#   $3 - substitute regex
#   $4 - use POSIX regex. Default: true
# Returns:
#   None
#########################
replace_in_file() {
    local filename="${1:?filename is required}"
    local match_regex="${2:?match regex is required}"
    local substitute_regex="${3:?substitute regex is required}"
    local posix_regex=${4:-true}

    local result

    # We should avoid using 'sed in-place' substitutions
    # 1) They are not compatible with files mounted from ConfigMap(s)
    # 2) We found incompatibility issues with Debian10 and "in-place" substitutions
    local -r del=$'\001' # Use a non-printable character as a 'sed' delimiter to avoid issues
    if [[ $posix_regex = true ]]; then
        result="$(sed -E "s${del}${match_regex}${del}${substitute_regex}${del}g" "$filename")"
    else
        result="$(sed "s${del}${match_regex}${del}${substitute_regex}${del}g" "$filename")"
    fi
    echo "$result" > "$filename"
}

########################
# Remove a line in a file based on a regex
# Arguments:
#   $1 - filename
#   $2 - match regex
#   $3 - use POSIX regex. Default: true
# Returns:
#   None
#########################
remove_in_file() {
    local filename="${1:?filename is required}"
    local match_regex="${2:?match regex is required}"
    local posix_regex=${3:-true}
    local result

    # We should avoid using 'sed in-place' substitutions
    # 1) They are not compatible with files mounted from ConfigMap(s)
    # 2) We found incompatibility issues with Debian10 and "in-place" substitutions
    if [[ $posix_regex = true ]]; then
        result="$(sed -E "/$match_regex/d" "$filename")"
    else
        result="$(sed "/$match_regex/d" "$filename")"
    fi
    echo "$result" > "$filename"
}

########################
# Appends text after the last line matching a pattern
# Arguments:
#   $1 - file
#   $2 - match regex
#   $3 - contents to add
# Returns:
#   None
#########################
append_file_after_last_match() {
    local file="${1:?missing file}"
    local match_regex="${2:?missing pattern}"
    local value="${3:?missing value}"

    # We read the file in reverse, replace the first match (0,/pattern/s) and then reverse the results again
    result="$(tac "$file" | sed -E "0,/($match_regex)/s||${value}\n\1|" | tac)"
    echo "$result" > "$file"
}

########################
# Replace a regex-matching string in a file in-place
# Arguments:
#   $1 - filename
#   $2 - match regex
#   $3 - substitute regex
#   $4 - use POSIX regex. Default: false
# Returns:
#   None
#########################
update_in_file() {
    local filename="${1:?filename is required}"
    local match_regex="${2:?match regex is required}"
    local substitute_regex="${3:?substitute regex is required}"
    local posix_regex=${4:-false}

    # We should avoid using 'sed in-place' substitutions
    # 1) They are not compatible with files mounted from ConfigMap(s)
    # 2) We found incompatibility issues with Debian10 and "in-place" substitutions
    local -r del=$'\001' # Use a non-printable character as a 'sed' delimiter to avoid issues
    if [[ $posix_regex = true ]]; then
        sed -i -E "s${del}${match_regex}${del}${substitute_regex}${del}g" "$filename"
    else
        sed -i "s${del}${match_regex}${del}${substitute_regex}${del}g" "$filename"
    fi
}
