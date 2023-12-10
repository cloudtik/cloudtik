#!/bin/bash
# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

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
    "${REDIS_BASE_DIR}/bin/redis-cli" --version | grep -E -o "[0-9]+.[0-9]+.[0-9]+"
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

# check to see if this file is being run or sourced from another script
_is_sourced() {
	# https://unix.stackexchange.com/a/215279
	[ "${#FUNCNAME[@]}" -ge 2 ] \
		&& [ "${FUNCNAME[0]}" = '_is_sourced' ] \
		&& [ "${FUNCNAME[1]}" = 'source' ]
}

redis_configure_default() {
    if [[ -n "$REDIS_PASSWORD" ]]; then
        redis_conf_set requirepass "$REDIS_PASSWORD"
    else
        redis_conf_unset requirepass
        # Allow remote connections without password
        redis_conf_set protected-mode no
    fi
}

redis_configure_replication() {
    if [[ -n "$REDIS_PASSWORD" ]]; then
        redis_conf_set masterauth "$REDIS_PASSWORD"
    fi
    # Configuring replication mode
    if [ "${REDIS_MASTER_NODE}" != "true" ]; then
        redis_conf_set "replicaof" "$REDIS_MASTER_HOST $REDIS_SERVICE_PORT"
    fi
}

_main() {
		# Init script for Redis Server started.
    redis_configure_default
    if [ "${REDIS_CLUSTER_MODE}" == "replication" ]; then
        redis_configure_replication
    fi
}

# If we are sourced from elsewhere, don't perform any further actions
if ! _is_sourced; then
  _main "$@"
fi
