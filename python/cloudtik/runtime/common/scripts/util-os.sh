#!/bin/bash
# Copyright VMware, Inc.
# SPDX-License-Identifier: APACHE-2.0
#
# Library for operating system actions

# shellcheck disable=SC1091

# Functions

########################
# Check if an user exists in the system
# Arguments:
#   $1 - user
# Returns:
#   Boolean
#########################
user_exists() {
    local user="${1:?user is missing}"
    id "$user" >/dev/null 2>&1
}

########################
# Check if a group exists in the system
# Arguments:
#   $1 - group
# Returns:
#   Boolean
#########################
group_exists() {
    local group="${1:?group is missing}"
    getent group "$group" >/dev/null 2>&1
}

########################
# Check if the script is currently running as root
# Arguments:
#   $1 - user
#   $2 - group
# Returns:
#   Boolean
#########################
am_i_root() {
    if [[ "$(id -u)" = "0" ]]; then
        true
    else
        false
    fi
}

########################
# Get total memory available
# Arguments:
#   None
# Returns:
#   Memory in bytes
#########################
get_total_memory() {
    echo $(($(grep MemTotal /proc/meminfo | awk '{print $2}') / 1024))
}

#########################
# Redirects output to /dev/null if debug mode is disabled
# Globals:
#   CLOUDTIK_SCRIPT_DEBUG
# Arguments:
#   $@ - Command to execute
# Returns:
#   None
#########################
execute_command() {
    local bool="${CLOUDTIK_SCRIPT_DEBUG:-false}"
    # comparison is performed without regard to the case of alphabetic characters
    shopt -s nocasematch
    if [[ "$bool" = 1 || "$bool" =~ ^(yes|true)$ ]]; then
        "$@"
    else
        "$@" >/dev/null 2>&1
    fi
}

########################
# Retries a command a given number of times
# Arguments:
#   $1 - cmd (as a string)
#   $2 - max retries. Default: 20
#   $3 - sleep between retries (in seconds). Default: 3
# Returns:
#   Boolean
#########################
retry_while() {
    local cmd="${1:?cmd is missing}"
    local retries="${2:-20}"
    local sleep_time="${3:-3}"
    local return_value=1

    read -r -a command <<<"$cmd"
    for ((i = 1; i <= retries; i += 1)); do
        "${command[@]}" && return_value=0 && break
        sleep "$sleep_time"
    done
    return $return_value
}

########################
# Create md5 hash from a string
# Arguments:
#   $1 - string
# Returns:
#   md5 hash - string
#########################
generate_md5_hash() {
    local -r str="${1:?missing input string}"
    echo -n "$str" | md5sum | awk '{print $1}'
}

########################
# Create sha1 hash from a string
# Arguments:
#   $1 - string
#   $2 - algorithm - 1 (default), 224, 256, 384, 512
# Returns:
#   sha1 hash - string
#########################
generate_sha_hash() {
    local -r str="${1:?missing input string}"
    local -r algorithm="${2:-1}"
    echo -n "$str" | "sha${algorithm}sum" | awk '{print $1}'
}

########################
# Get the deb arch of the system
# Returns:
#   deb arch - string
#########################
get_deb_arch() {
    local deb_arch="amd64"
    arch=$(uname -m)
    if [ "${arch}" == "aarch64" ]; then
        deb_arch="arm64"
    fi
    echo "${deb_arch}"
}
