#!/bin/bash
#
# This script checks if redis server health and whether it is a primary server
# or secondary server for replication.
#
# if request has no path or with /primary, /master
# response:
# "HTTP/1.1 200 OK" (if running as primary)
# - OR -
# "HTTP/1.1 503 Service Unavailable" (else)
#
# if request has path with /secondary, /slave
# response:
# "HTTP/1.1 200 OK" (if is running as secondary)
# - OR -
# "HTTP/1.1 503 Service Unavailable" (if not running as secondary)
#
# if request has path with /any or other paths
# response:
# "HTTP/1.1 200 OK" (if is running as primary or secondary)
# - OR -
# "HTTP/1.1 503 Service Unavailable" (if service is not available)
#
# Tests: echo "GET /primary HTTP/1.0" | redis-health-check.sh

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"
RUNTIME_PATH=/home/$(whoami)/runtime
REDIS_HOME=$RUNTIME_PATH/redis

# health check common functions
. "$ROOT_DIR"/common/scripts/util-health-check.sh

. "$REDIS_HOME"/etc/redis

redis_execute_with_output () {
    local -r host="localhost"
    local -r port="${REDIS_PORT}"
    local -r password="${REDIS_PASSWORD}"
    local opts
    read -r -a opts <<<"${@:1}"

    local args=("-h" "$host" "-p" "$port")
    [[ -n "$password" ]] && args+=("-a" "$password" "--no-auth-warning")
    [[ "${#opts[@]}" -gt 0 ]] && args+=("${opts[@]}")

    # Execute cli with arguments
    /usr/bin/redis-cli "${args[@]}" 2>/dev/null
}

_get_replication_server_role() {
    local role_info
    if ! role_info="$(redis_execute_with_output --raw role)"; then
        return 1
    fi
    if [ -z "$role_info" ]; then
        return 1
    fi

    local role_fields=($role_info)
    local replication_role="${role_fields[0]}"
    if [ "$replication_role" == "master" ]; then
        echo "primary"
    elif [ "$replication_role" == "slave" ]; then
        echo "secondary"
    else
        return 1
    fi
}

_get_sharding_server_role() {
    local cluster_nodes
    if ! cluster_nodes="$(redis_execute_with_output cluster nodes)"; then
        return 1
    fi
    if [ -z "$cluster_nodes" ]; then
        return 1
    fi
    for cluster_node in $(echo "${cluster_nodes}" | tr ' ' '|'); do
        IFS="|" read -ra node_fields <<< "$cluster_node"
        local node_flag_str="${node_fields[2]}"
        IFS="," read -ra node_flags <<< "$node_flag_str"

        if [[ ${node_flags[@]} =~ "myself" ]]; then
            # this node
            if [[ ${node_flags[@]} =~ "master" ]]; then
                echo "primary"
                return 0
            elif [[ ${node_flags[@]} =~ "slave" ]]; then
                echo "secondary"
                return 0
            else
                return 1
            fi
        fi
    done
    return 1
}

_get_standalone_server_role() {
    local query_result
    if ! query_result="$(redis_execute_with_output ping)"; then
        return 1
    fi
    if [ "$query_result" == "PONG" ]; then
        echo "primary"
    else
        return 1
    fi
}

_get_server_role() {
    local server_role
    if [ "${REDIS_CLUSTER_MODE}" == "replication" ]; then
        if ! server_role=$(_get_replication_server_role); then
            return 1
        fi
    elif [ "${REDIS_CLUSTER_MODE}" == "sharding" ]; then
        if ! server_role=$(_get_sharding_server_role); then
            return 1
        fi
    else
        if ! server_role=$(_get_standalone_server_role); then
            return 1
        fi
    fi
    echo "$server_role"
}

_main() {
    if [ ! -z "$1" ]; then
        http_parse_request_uri "$1"
    fi
    http_read

    local server_role
    if ! server_role=$(_get_server_role); then
        response 503 "FAIL: Failed to get server role."
    fi
    if [[ -z "${HTTP_REQ_URI_PATH}" ]] \
        || [[ "${HTTP_REQ_URI_PATH}" == "/" ]] \
        || [[ "${HTTP_REQ_URI_PATH}" == "/primary" ]] \
        || [[ "${HTTP_REQ_URI_PATH}" == "/master" ]]; then
        if [[ "${server_role}" == "primary" ]]; then
            response 200 "OK: ${server_role}"
        fi
    elif [[ "${HTTP_REQ_URI_PATH}" == "/secondary" ]] \
        || [[ "${HTTP_REQ_URI_PATH}" == "/slave" ]]; then
        if [[ "${server_role}" == "secondary" ]]; then
            response 200 "OK: ${server_role}"
        fi
    else
        if [[ "${server_role}" == "primary" ]] \
            || [[ "${server_role}" == "secondary" ]]; then
            response 200 "OK: ${server_role}"
        fi
    fi

    response 503 "FAIL: ${server_role}"
}

_main "$@"
