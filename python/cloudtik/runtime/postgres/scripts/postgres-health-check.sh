#!/bin/bash
#
# This script checks if postgres server is a primary server or standby server
#
# if request has no path or with /primary
# response:
# "HTTP/1.1 200 OK" (if postgres is running as primary)
# - OR -
# "HTTP/1.1 503 Service Unavailable" (else)
#
# if request has path with /secondary, /standby
# response:
# "HTTP/1.1 200 OK" (if postgres is running as standby)
# - OR -
# "HTTP/1.1 503 Service Unavailable" (if not running as standby)
#
# if request has path with /any or other paths
# response:
# "HTTP/1.1 200 OK" (if postgres is running as primary or standby)
# - OR -
# "HTTP/1.1 503 Service Unavailable" (if service is not available)
#

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"
RUNTIME_PATH=/home/$(whoami)/runtime
POSTGRES_HOME=$RUNTIME_PATH/postgres

# health check common functions
. "$ROOT_DIR"/common/scripts/util-health-check.sh

. "$POSTGRES_HOME"/conf/postgres

_get_server_role() {
    # pg_is_in_recovery function explained below
    # Name 			Return Type 	Description
    # pg_is_in_recovery() 	bool 		True if recovery is still in progress.
    local -r port="${POSTGRES_PORT:-5432}"
    local -r user="${POSTGRES_REPLICATION_USER:-repl_user}"
    local -r password="${POSTGRES_REPLICATION_PASSWORD}"
    local -r database="postgres"
    local in_recovery
    if ! in_recovery="$( PGPASSWORD=$password /usr/bin/psql \
      -tA -h localhost -p ${port} -U ${user} -d ${database} \
      -c 'select pg_is_in_recovery();' 2>/dev/null )"; then
        return 1
    fi
    if [ "$in_recovery" = "t" ]; then
        echo "standby"
    elif [ "$in_recovery" = "f" ]; then
        echo "primary"
    else
        echo "unknown"
    fi
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
    elif [[ "${HTTP_REQ_URI_PATH}" == "/secondary" ]] \
        || [[ "${HTTP_REQ_URI_PATH}" == "/standby" ]]; then
        if [[ "${server_role}" == "standby" ]]; then
            response 200 "OK: ${server_role}"
        fi
    else
        if [[ "${server_role}" == "primary" ]] \
            || [[ "${server_role}" == "standby" ]]; then
            response 200 "OK: ${server_role}"
        fi
    fi

    response 503 "FAIL: ${server_role}"
}

_main "$@"
