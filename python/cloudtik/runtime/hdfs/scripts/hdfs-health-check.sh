#!/bin/bash
#
# This script checks if HDFS HA name cluster health based on role.
# Whether a NameNode is a active or standby server.
#
# if request has no path or with /primary, /active
# response:
# "HTTP/1.1 200 OK" (if running as primary)
# - OR -
# "HTTP/1.1 503 Service Unavailable" (else)
#
# if request has path with /secondary, /standby
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

# These are needed for running hdfs commands
export HADOOP_HOME=$RUNTIME_PATH/hadoop
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hdfs
export JAVA_HOME=$RUNTIME_PATH/jdk

# health check common functions
. "$ROOT_DIR"/common/scripts/util-health-check.sh

. ${HADOOP_CONF_DIR}/hdfs

hdfs_ha_service_state () {
    local -r name_service_id="${HDFS_NAME_SERVICE_ID}"
    # Execute haadmin command for service state
    $HADOOP_HOME/bin/hdfs haadmin -getServiceState ${name_service_id} 2>/dev/null
}

hdfs_name_node_state() {
    local host=${HDFS_NODE_IP}
    local port=${HDFS_HTTP_PORT:-9870}
    local state
    state=`curl -s "http://$host:$port/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus" | grep '\"State\" :'`
    local ret=$?
    [ $ret -ne 0 ] &&  { return $ret; }
    state=${state//\"/}
    state=${state//,/}
    state=${state##*:}
    state=${state// /}
    echo "$state"
}

_get_ha_cluster_server_role() {
    local name_role
    if ! name_role="$(hdfs_ha_service_state)"; then
        return 1
    fi
    if [ "${name_role}" == "active" ]; then
        echo "active"
    elif [ "${name_role}" == "standby" ]; then
        echo "standby"
    else
        return 1
    fi
}

_get_simple_server_role() {
    local state
    if ! state="$(hdfs_name_node_state)"; then
        return 1
    fi
    if [ "$state" == "active" ]; then
        echo "active"
    else
        return 1
    fi
}

_get_server_role() {
    local server_role
    if [ "${HDFS_CLUSTER_MODE}" == "ha_cluster" ]; then
        if ! server_role=$(_get_ha_cluster_server_role); then
            return 1
        fi
    else
        if ! server_role=$(_get_simple_server_role); then
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
        || [[ "${HTTP_REQ_URI_PATH}" == "/active" ]]; then
        if [[ "${server_role}" == "active" ]]; then
            response 200 "OK: ${server_role}"
        fi
    elif [[ "${HTTP_REQ_URI_PATH}" == "/secondary" ]] \
        || [[ "${HTTP_REQ_URI_PATH}" == "/standby" ]]; then
        if [[ "${server_role}" == "standby" ]]; then
            response 200 "OK: ${server_role}"
        fi
    else
        if [[ "${server_role}" == "active" ]] \
            || [[ "${server_role}" == "standby" ]]; then
            response 200 "OK: ${server_role}"
        fi
    fi

    response 503 "FAIL: ${server_role}"
}

_main "$@"
