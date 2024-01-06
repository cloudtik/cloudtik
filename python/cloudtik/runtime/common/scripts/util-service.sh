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
#   $1 - Pid
#   $2 - Signal number (optional)
# Returns:
#   None
#########################
stop_service_by_pid() {
    local pid="${1:?pid is missing}"
    local signal="${2:-}"

    ! is_service_running "$pid" && return

    if [[ -n "$signal" ]]; then
        kill "-${signal}" "$pid" >/dev/null 2>&1
    else
        kill "$pid" >/dev/null 2>&1
    fi

    local counter=10
    while [[ "$counter" -ne 0 ]] && is_service_running "$pid"; do
        sleep 1
        counter=$((counter - 1))
    done
}

########################
# Stop a service by sending a termination signal to its pid
# Arguments:
#   $1 - Pid file
#   $2 - Signal number (optional)
# Returns:
#   None
#########################
stop_service_by_pid_file() {
    local pid_file="${1:?pid file is missing}"
    local signal="${2:-}"
    local pid

    pid="$(get_pid_from_file "$pid_file")"
    [[ -z "$pid" ]] && return

    stop_service_by_pid "$pid" "$signal"
}

stop_process_by_name() {
    local process_name="${1:?process name is missing}"
    local signal="${2:-}"
    local pid=$(pgrep "${process_name}")
    [[ -z "$pid" ]] && return

    echo "Stopping ${process_name}..."
    stop_service_by_pid "$pid" "$signal"
}

stop_process_by_command() {
    local process_cmd="${1:?command is missing}"
    local signal="${2:-}"
    local pid=$(pgrep -f "${process_cmd}")
    [[ -z "$pid" ]] && return
    stop_service_by_pid "$pid" "$signal"
}

stop_process_by_pid_file() {
    local pid_file="${1:?pid file is missing}"
    local signal="${2:-}"
    if sudo test -f "$pid_file"; then
        local process_name=$(basename "$pid_file")
        echo "Stopping ${process_name}..."
        stop_service_by_pid_file "$pid_file" "$signal"
    fi
}
