#!/bin/bash
# Copyright VMware, Inc.
# SPDX-License-Identifier: APACHE-2.0
#
# Library for network functions

# shellcheck disable=SC1091

# Functions
# Depending on util-log, util-os
# HTTP depending on curl

########################
# Resolve IP address for a host/domain (i.e. DNS lookup)
# Arguments:
#   $1 - Hostname to resolve
#   $2 - IP address version (v4, v6), leave empty for resolving to any version
# Returns:
#   IP
#########################
dns_lookup() {
    local host="${1:?host is missing}"
    local ip_version="${2:-}"
    getent "ahosts${ip_version}" "$host" | awk '/STREAM/ {print $1 }' | head -n 1
}

#########################
# Wait for a hostname and return the IP
# Arguments:
#   $1 - hostname
#   $2 - number of retries
#   $3 - seconds to wait between retries
# Returns:
#   - IP address that corresponds to the hostname
#########################
wait_for_dns_lookup() {
    local hostname="${1:?hostname is missing}"
    local retries="${2:-5}"
    local seconds="${3:-1}"
    check_host() {
        if [[ $(dns_lookup "$hostname") == "" ]]; then
            false
        else
            true
        fi
    }
    # Wait for the host to be ready
    retry_while "check_host ${hostname}" "$retries" "$seconds"
    dns_lookup "$hostname"
}

########################
# Get machine's IP
# Arguments:
#   None
# Returns:
#   Machine IP
#########################
get_machine_ip() {
    local -a ip_addresses
    local hostname
    hostname="$(hostname)"
    read -r -a ip_addresses <<< "$(dns_lookup "$hostname" | xargs echo)"
    if [[ "${#ip_addresses[@]}" -gt 1 ]]; then
        warn "Found more than one IP address associated to hostname ${hostname}: ${ip_addresses[*]}, will use ${ip_addresses[0]}"
    elif [[ "${#ip_addresses[@]}" -lt 1 ]]; then
        error "Could not find any IP address associated to hostname ${hostname}"
        exit 1
    fi
    echo "${ip_addresses[0]}"
}

########################
# Check if the provided argument is a resolved hostname
# Arguments:
#   $1 - Value to check
# Returns:
#   Boolean
#########################
is_hostname_resolved() {
    local -r host="${1:?missing value}"
    if [[ -n "$(dns_lookup "$host")" ]]; then
        true
    else
        false
    fi
}

########################
# Wait for a HTTP connection to succeed
# Globals:
#   *
# Arguments:
#   $1 - URL to wait for
#   $2 - Maximum amount of retries (optional)
#   $3 - Time between retries (optional)
# Returns:
#   true if the HTTP connection succeeded, false otherwise
#########################
wait_for_http_connection() {
    local url="${1:?missing url}"
    local retries="${2:-}"
    local sleep_time="${3:-}"
    if ! retry_while "execute_command curl --silent ${url}" "$retries" "$sleep_time"; then
        error "Could not connect to ${url}"
        return 1
    fi
}

########################
# Wait for port
# Globals:
#   *
# Arguments:
#   $1 - Port to wait for
#   $2 - Host of the port (optional)
#   $3 - Time seconds (optional)
# Returns:
#   Boolean
#########################
wait_for_port() {
    local port="${1:?missing port to wait for}"
    local host="${2:-}"
    local timeout="${3:-}"
    local -a args=()
    [[ -n "$host" ]] && args+=("--host" "$host")
    [[ -n "$timeout" ]] && args+=("--timeout" "$timeout")
    cloudtik node wait-for-port $port "${args[@]}"
}

########################
# Checks system is managed by systemd
# arguments:
#   None
# returns:
#   boolean
#########################
is_init_systemd() {
    local -r init_program="$(ps --no-headers -o comm 1)"
    if [[ "${init_program}" == "systemd" ]]; then
        true
    else
        false
    fi
}

########################
# Checks whether systemd-resolved is active
# arguments:
#   None
# returns:
#   boolean
#########################
is_systemd_resolved_active() {
    if ! is_init_systemd; then
        false
    else
        local -r resolved_status="$(systemctl is-active systemd-resolved)"
        if [[ "${resolved_status}" == "active" ]]; then
            true
        else
            false
        fi
    fi
}
