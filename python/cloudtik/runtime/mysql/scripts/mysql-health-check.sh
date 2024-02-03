#!/bin/bash
#
# This script checks if mysql server health and whether it is a primary server
# or secondary server for group replication.
#
# if request has no path or with /primary
# response:
# "HTTP/1.1 200 OK" (if running as primary)
# - OR -
# "HTTP/1.1 503 Service Unavailable" (else)
#
# if request has path with /secondary
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
# Tests: echo "GET /primary HTTP/1.0" | mysql-health-check.sh

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"
RUNTIME_PATH=/home/$(whoami)/runtime
MYSQL_HOME=$RUNTIME_PATH/mysql

# health check common functions
. "$ROOT_DIR"/common/scripts/util-health-check.sh

. "$MYSQL_HOME"/conf/mysql

mysql_execute_with_output () {
    local -r port="${MYSQL_PORT:-3306}"
    local -r user="root"
    local -r password="${MYSQL_ROOT_PASSWORD}"
    local -r database="mysql"
    /usr/bin/mysql \
      -h localhost -P ${port} -u ${user} --password=$password -D ${database} -N -s 2>/dev/null
}

_get_replication_server_role() {
    local replication_status
    if ! replication_status="$(mysql_execute_with_output \
      <<< "SHOW REPLICA STATUS;")"; then
        return 1
    fi

    if [ "$replication_status" == "" ]; then
        echo "primary"
    else
        echo "secondary"
    fi
}

_get_group_replication_server_role() {
    local replication_group_members
    if ! replication_group_members="$(mysql_execute_with_output \
      <<< "SELECT MEMBER_ID,MEMBER_HOST,MEMBER_PORT,MEMBER_STATE,MEMBER_ROLE FROM performance_schema.replication_group_members;")"; then
        return 1
    fi
    if [ "$replication_group_members" == "" ]; then
        return 1
    fi
    local -r my_hostname="$(hostname)"
    for replication_group_member in $(echo "${replication_group_members}" | tr '\t' '|'); do
        # if this is my hostname, getting its role
        IFS="|" read -ra member_info <<< "$replication_group_member"
        local member_host="${member_info[1]}"
        local member_state="${member_info[3]}"
        local member_role="${member_info[4]}"
        if [ "$member_host" == "$my_hostname" ]; then
            if [ "$member_state" == "ONLINE" ]; then
                # PRIMARY or SECONDARY role convert to lower case
                echo $(echo $member_role | tr '[:upper:]' '[:lower:]')
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
    if ! query_result="$(mysql_execute_with_output <<< "SHOW DATABASES;")"; then
        return 1
    fi
    if [ "$query_result" != "" ]; then
        echo "primary"
    else
        return 1
    fi
}

_get_server_role() {
    local server_role
    if [ "${MYSQL_CLUSTER_MODE}" == "replication" ]; then
        if ! server_role=$(_get_replication_server_role); then
            return 1
        fi
    elif [ "${MYSQL_CLUSTER_MODE}" == "group_replication" ]; then
        if ! server_role=$(_get_group_replication_server_role); then
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
        || [[ "${HTTP_REQ_URI_PATH}" == "/primary" ]]; then
        if [[ "${server_role}" == "primary" ]]; then
            response 200 "OK: ${server_role}"
        fi
    elif [[ "${HTTP_REQ_URI_PATH}" == "/secondary" ]]; then
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
