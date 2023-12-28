#!/bin/bash
# Copyright VMware, Inc.
# SPDX-License-Identifier: APACHE-2.0
#
# shellcheck disable=SC1090,SC1091

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

# Load util functions
. "$ROOT_DIR"/common/scripts/utils.sh

# Functions

########################
# Set Elasticsearch keystore values
# Globals:
#   ELASTICSEARCH_KEYS
# Arguments:
#   None
# Returns:
#   None
#########################
elasticsearch_set_keys() {
    read -r -a keys_list <<<"$(tr ',;' ' ' <<<"$ELASTICSEARCH_KEYS")"
    if [[ "${#keys_list[@]}" -gt 0 ]]; then
        for key_value in "${keys_list[@]}"; do
            read -r -a key_value <<<"$(tr '=' ' ' <<<"$key_value")"
            local key="${key_value[0]}"
            local value="${key_value[1]}"

            elasticsearch_set_key_value "$key" "$value"
        done
    fi
}

########################
# Set Elasticsearch keystore values
# Globals:
#   ELASTICSEARCH_*
# Arguments:
#   None
# Returns:
#   None
#########################
elasticsearch_set_key_value() {
    local key="${1:?missing key}"
    local value="${2:?missing value}"

    debug "Storing key: ${key}"
    elasticsearch-keystore add --stdin --force "$key" <<<"$value" >/dev/null 2>&1

    # Avoid exit code of previous commands to affect the result of this function
    true
}

########################
# Write a configuration setting value (need yq installed)
# Globals:
#   ELASTICSEARCH_CONF_FILE
# Arguments:
#   $1 - key
#   $2 - value
#   $3 - YAML type (string, int or bool)
# Returns:
#   None
#########################
elasticsearch_conf_write() {
    local -r key="${1:?Missing key}"
    local -r value="${2:-}"
    local -r type="${3:-string}"
    local -r tempfile=$(mktemp)

    case "$type" in
    string)
        yq eval "(.${key}) |= \"${value}\"" "$ELASTICSEARCH_CONF_FILE" >"$tempfile"
        ;;
    int)
        yq eval "(.${key}) |= ${value}" "$ELASTICSEARCH_CONF_FILE" >"$tempfile"
        ;;
    bool)
        yq eval "(.${key}) |= (\"${value}\" | test(\"true\"))" "$ELASTICSEARCH_CONF_FILE" >"$tempfile"
        ;;
    *)
        error "Type unknown: ${type}"
        return 1
        ;;
    esac
    cp "$tempfile" "$ELASTICSEARCH_CONF_FILE"
}

########################
# Set a configuration setting value
# Globals:
#   ELASTICSEARCH_CONF_FILE
# Arguments:
#   $1 - key
#   $2 - values (array)
# Returns:
#   None
#########################
elasticsearch_conf_set() {
    local key="${1:?missing key}"
    shift
    local values=("${@}")

    if [[ "${#values[@]}" -eq 0 ]]; then
        stderr_print "$key"
        stderr_print "missing values"
        return 1
    elif [[ "${#values[@]}" -eq 1 ]] && [[ -n "${values[0]}" ]]; then
        elasticsearch_conf_write "$key" "${values[0]}"
    else
        for i in "${!values[@]}"; do
            if [[ -n "${values[$i]}" ]]; then
                elasticsearch_conf_write "${key}[$i]" "${values[$i]}"
            fi
        done
    fi
}

########################
# Determine the hostname by which Elasticsearch can be contacted
# Returns:
#   The value of $ELASTICSEARCH_ADVERTISED_HOSTNAME or the current host address
########################
get_elasticsearch_hostname() {
    if [[ -n "$ELASTICSEARCH_ADVERTISED_HOSTNAME" ]]; then
        echo "$ELASTICSEARCH_ADVERTISED_HOSTNAME"
    else
        get_machine_ip
    fi
}

########################
# Check if Elasticsearch is running
# Globals:
#   ELASTICSEARCH_PID_FILE
# Arguments:
#   None
# Returns:
#   Boolean
#########################
is_elasticsearch_running() {
    local pid
    pid="$(get_pid_from_file "$ELASTICSEARCH_PID_FILE")"

    if [[ -z "$pid" ]]; then
        false
    else
        is_service_running "$pid"
    fi
}

########################
# Check if Elasticsearch is not running
# Globals:
#   ELASTICSEARCH_PID_FILE
# Arguments:
#   None
# Returns:
#   Boolean
#########################
is_elasticsearch_not_running() {
    ! is_elasticsearch_running
    return "$?"
}

########################
# Stop Elasticsearch
# Globals:
#   ELASTICSEARCH_PID_FILE
# Arguments:
#   None
# Returns:
#   None
#########################
elasticsearch_stop() {
    ! is_elasticsearch_running && return
    debug "Stopping ElasticSearch..."
    stop_service_by_pid_file "$ELASTICSEARCH_PID_FILE"
}

########################
# Start Elasticsearch and wait until it's ready
# Globals:
#   ELASTICSEARCH_*
# Arguments:
#   None
# Returns:
#   None
#########################
elasticsearch_start() {
    is_elasticsearch_running && return

    debug "Starting ElasticSearch..."
    local command=("elasticsearch" "-d" "-p" "$ELASTICSEARCH_PID_FILE")
    if [[ "$CLOUDTIK_SCRIPT_DEBUG" = true ]]; then
        "${command[@]}" &
    else
        "${command[@]}" >/dev/null 2>&1 &
    fi

    local retries=50
    local seconds=2
    # Check the process is running
    retry_while "is_elasticsearch_running" "$retries" "$seconds"
    # Check Elasticsearch API is reachable
    retry_while "elasticsearch_healthcheck" "$retries" "$seconds"
}

########################
# Check Elasticsearch/Opensearch health
# Globals:
#   DB_*
# Arguments:
#   None
# Returns:
#   0 when healthy
#   1 when unhealthy
#########################
elasticsearch_healthcheck() {
    info "Checking ElasticSearch health..."
    local -r cmd="curl"
    local command_args=("--silent" "--write-out" "%{http_code}")
    local protocol="http"
    local host
    local username="elastic"

    host=$(get_elasticsearch_hostname)

    if [[ ! -z "${ELASTICSEARCH_USERNAME}" ]]; then
        username="${ELASTICSEARCH_USERNAME}"
    fi

    if [[ "${ELASTICSEARCH_SECURITY}" == true ]]; then
        command_args+=("-k" "--user" "${username}:${ELASTICSEARCH_PASSWORD}")
        protocol="https"
    fi

    # Combination of --silent, --output and --write-out allows us to obtain both the status code and the request body
    output=$(mktemp)
    command_args+=("-o" "$output" "${protocol}://${host}:${ELASTICSEARCH_SERVICE_PORT}/_cluster/health?local=true")
    HTTP_CODE=$("$cmd" "${command_args[@]}")
    if [[ ${HTTP_CODE} -ge 200 && ${HTTP_CODE} -le 299 ]]; then
        rm "$output"
        return 0
    else
        rm "$output"
        return 1
    fi
}
