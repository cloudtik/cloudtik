#!/bin/bash
# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

# Load util functions
. "$ROOT_DIR"/common/scripts/utils.sh

# Functions

########################
# Retrieve a configuration setting value
# Globals:
#   REDIS_BASE_DIR
# Arguments:
#   $1 - key
#   $2 - conf file
# Returns:
#   None
#########################
redis_conf_get() {
    local -r key="${1:?missing key}"
    local -r conf_file="${2:-"${REDIS_BASE_DIR}/etc/redis.conf"}"

    if grep -q -E "^\s*$key " "$conf_file"; then
        grep -E "^\s*$key " "$conf_file" | awk '{print $2}'
    fi
}

########################
# Set a configuration setting value
# Globals:
#   REDIS_BASE_DIR
#   REDIS_CONF_SET_FILE
# Arguments:
#   $1 - key
#   $2 - value
# Returns:
#   None
#########################
redis_conf_set() {
    local -r key="${1:?missing key}"
    local value="${2:-}"
    local -r conf_file="${REDIS_CONF_SET_FILE:-"${REDIS_BASE_DIR}/etc/redis.conf"}"

    # Sanitize inputs
    value="${value//\\/\\\\}"
    value="${value//&/\\&}"
    value="${value//\?/\\?}"
    value="${value//[$'\t\n\r']}"
    [[ "$value" = "" ]] && value="\"$value\""

    # Determine whether to enable the configuration for RDB persistence, if yes, do not enable the replacement operation
    if [ "${key}" == "save" ]; then
        echo "${key} ${value}" >> "${conf_file}"
    else
        replace_in_file "${conf_file}" "^#*\s*${key} .*" "${key} ${value}" false
    fi
}

########################
# Unset a configuration setting value
# Globals:
#   REDIS_BASE_DIR
# Arguments:
#   $1 - key
#   $2 - conf file
# Returns:
#   None
#########################
redis_conf_unset() {
    local -r key="${1:?missing key}"
    local -r conf_file="${2:-"${REDIS_BASE_DIR}/etc/redis.conf"}"
    remove_in_file "${conf_file}" "^\s*$key .*" false
}

########################
# Get Redis version
# Globals:
#   REDIS_BASE_DIR
# Arguments:
#   None
# Returns:
#   Redis versoon
#########################
redis_version() {
    redis-cli --version | grep -E -o "[0-9]+.[0-9]+.[0-9]+"
}

########################
# Get Redis major version
# Globals:
#   REDIS_BASE_DIR
# Arguments:
#   None
# Returns:
#   Redis major version
#########################
redis_major_version() {
    redis_version | grep -E -o "^[0-9]+"
}

########################
# Check if redis is running
# Globals:
#   REDIS_BASE_DIR
# Arguments:
#   $1 - pid file
# Returns:
#   Boolean
#########################
is_redis_running() {
    local pid_file="${1:-"${REDIS_BASE_DIR}/redis-server.pid"}"
    local pid
    pid="$(get_pid_from_file "$pid_file")"

    if [[ -z "$pid" ]]; then
        false
    else
        is_service_running "$pid"
    fi
}

########################
# Check if redis is not running
# Globals:
#   REDIS_BASE_DIR
# Arguments:
#   $1 - pid file
# Returns:
#   Boolean
#########################
is_redis_not_running() {
    ! is_redis_running "$@"
}

########################
# Configure general options
# Globals:
#   REDIS_*
# Arguments:
#   None
# Returns:
#   None
#########################
redis_configure_default() {
    if [[ -n "$REDIS_PASSWORD" ]]; then
        redis_conf_set requirepass "$REDIS_PASSWORD"
    else
        redis_conf_unset requirepass
        # Allow remote connections without password
        redis_conf_set protected-mode no
    fi
}

########################
# Configure replication options
# Globals:
#   REDIS_*
# Arguments:
#   None
# Returns:
#   None
#########################
redis_configure_replication() {
    if [[ -n "$REDIS_PASSWORD" ]]; then
        redis_conf_set masterauth "$REDIS_PASSWORD"
    fi
    # Configuring replication mode
    if [ "${REDIS_REPLICATION_ROLE}" == "primary" ]; then
        # remove replicaof for master
        redis_conf_unset replicaof
    else
        local primary_port=${REDIS_PRIMARY_PORT:-$REDIS_PORT}
        redis_conf_set "replicaof" "$REDIS_PRIMARY_HOST $primary_port"
    fi
}

########################
# Configure sharding options
# Globals:
#   REDIS_*
# Arguments:
#   None
# Returns:
#   None
#########################
redis_configure_sharding() {
    if [[ -n "$REDIS_PASSWORD" ]]; then
        redis_conf_set masterauth "$REDIS_PASSWORD"
    fi
    if [[ "$REDIS_NODE_IP" != "$REDIS_NODE_HOST" ]]; then
        redis_conf_set "cluster-announce-hostname" "$REDIS_NODE_HOST"
        redis_conf_set "cluster-preferred-endpoint-type" "hostname"
    else
        redis_conf_unset "cluster-announce-hostname"
        redis_conf_set "cluster-preferred-endpoint-type" "ip"
    fi
}

########################
# Execute commands through redis-cli
# Globals:
#   REDIS_*
# Arguments:
#   host, port and password
#   commands and options
# Returns:
#   Command result
#########################
redis_execute() {
    local -r host="${1:?missing host}"
    local -r port="${2:-$REDIS_PORT}"
    local -r password="${3:-}"
    local opts
    read -r -a opts <<<"${@:4}"

    local args=("-h" "$host" "-p" "$port")
    [[ -n "$password" ]] && args+=("-a" "$password" "--no-auth-warning")
    [[ "${#opts[@]}" -gt 0 ]] && args+=("${opts[@]}")

    # Execute cli with arguments
    redis-cli "${args[@]}" 2>/dev/null
}
