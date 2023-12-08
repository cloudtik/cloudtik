#!/bin/bash
# Copyright VMware, Inc.
# SPDX-License-Identifier: APACHE-2.0
#
# Library for managing services

# shellcheck disable=SC1091

# Functions

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

stop_process_by_name() {
    local PROCESS_NAME=$1
    local MY_PID=$(pgrep ${PROCESS_NAME})
    if [ -n "${MY_PID}" ]; then
        echo "Stopping ${PROCESS_NAME}..."
        # SIGTERM = 15
        sudo kill -15 ${MY_PID} >/dev/null 2>&1
    fi
}

stop_process_by_pid_file() {
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
