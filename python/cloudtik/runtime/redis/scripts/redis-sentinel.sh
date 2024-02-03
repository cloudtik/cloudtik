#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

# Load generic functions
. "$ROOT_DIR"/common/scripts/util-file.sh
. "$ROOT_DIR"/common/scripts/util-log.sh
. "$ROOT_DIR"/common/scripts/util-os.sh
. "$ROOT_DIR"/common/scripts/util-service.sh
. "$ROOT_DIR"/common/scripts/util-fs.sh
. "$ROOT_DIR"/common/scripts/util-value.sh

# redis functions
. "$BIN_DIR"/redis.sh

########################
# Set a configuration setting value
# Globals:
#   REDIS_SENTINEL_CONF_FILE
# Arguments:
#   $1 - key
#   $2 - value
# Returns:
#   None
#########################
redis_sentinel_conf_set() {
    local key="${1:?missing key}"
    local value="${2:-}"

    # Sanitize inputs
    value="${value//\\/\\\\}"
    value="${value//&/\\&}"
    value="${value//\?/\\?}"
    [[ "$value" = "" ]] && value="\"$value\""

    if grep -q "^\s*$key .*" "$REDIS_SENTINEL_CONF_FILE"; then
        replace_in_file "$REDIS_SENTINEL_CONF_FILE" "^\s*${key} .*" "${key} ${value}" false
    else
        printf '\n%s %s' "$key" "$value" >>"$REDIS_SENTINEL_CONF_FILE"
    fi
}

########################
# Check if redis sentinel is running
# Globals:
#   REDIS_BASE_DIR
# Arguments:
#   None
# Returns:
#   Boolean
#########################
is_redis_sentinel_running() {
    local pid_file="${1:-"${REDIS_BASE_DIR}/redis-sentinel.pid"}"
    local pid
    pid="$(get_pid_from_file "$pid_file")"

    if [[ -z "$pid" ]]; then
        false
    else
        is_service_running "$pid"
    fi
}

########################
# Check if redis sentinel is not running
# Globals:
#   REDIS_BASE_DIR
# Arguments:
#   None
# Returns:
#   Boolean
#########################
is_redis_sentinel_not_running() {
    ! is_redis_sentinel_running
}

########################
# Get sentinel master info
# Globals:
#   REDIS_*
# Arguments:
#   None
# Returns:
#   master info
#########################
redis_sentinel_master() {
    redis_execute "$@" "--raw" sentinel master "${REDIS_SENTINEL_MASTER_NAME}"
}

########################
# Ask seed nodes which node is the primary
# Globals:
#   REDIS_SENTINEL_*
# Arguments:
#   Non
# Returns:
#   String[] - (host port)
#########################
redis_sentinel_get_upstream_node() {
    local primary_conninfo
    local pretending_primary_id=""
    local pretending_primary_host=""
    local pretending_primary_port=""
    local host=""
    local port=""
    local suggested_primary_id=""
    local suggested_primary_host=""
    local suggested_primary_port=""

    if [[ -n "$REDIS_SENTINEL_SEED_NODES" ]]; then
        info "Querying all seed nodes for common upstream node..."
        read -r -a nodes <<<"$(tr ',;' ' ' <<<"${REDIS_SENTINEL_SEED_NODES}")"
        for node in "${nodes[@]}"; do
            host="$node"
            port="$REDIS_SENTINEL_PORT"
            debug "Checking node '$host:$port'..."
            if ! primary_info="$(redis_sentinel_master "$host" "$port" "$REDIS_PASSWORD")"; then
                debug "Skipping: failed to get primary from the node '$host:$port'!"
                continue
            elif [[ -z "$primary_info" ]]; then
                debug "Skipping: failed to get information about primary node!"
                continue
            else
                primary_fields=($primary_info)
                suggested_primary_host="${primary_fields[3]}"
                suggested_primary_port="${primary_fields[5]}"
                suggested_primary_id="${primary_fields[7]}"
                debug "Pretending primary role node - '${suggested_primary_host}'"
                if [[ -n "$pretending_primary_id" ]]; then
                    if [[ "${pretending_primary_id}" != "${suggested_primary_id}" ]]; then
                        warn "Conflict of pretending primary role nodes (previously: '${pretending_primary_id}', now: '${suggested_primary_id}')"
                        pretending_primary_id="" && pretending_primary_host="" && pretending_primary_port="" && break
                    fi
                else
                    debug "Pretending primary set to '${suggested_primary_host}'!"
                    pretending_primary_id="$suggested_primary_id"
                    pretending_primary_host="$suggested_primary_host"
                    pretending_primary_port="$suggested_primary_port"
                fi
            fi
        done
    fi

    echo "$pretending_primary_id"
    echo "$pretending_primary_host"
    echo "$pretending_primary_port"
}

########################
# Gets the node that is currently set as primary node
# Globals:
#   REDIS_SENTINEL_*
# Arguments:
#   None
# Returns:
#   String[] - (host port)
#########################
redis_sentinel_get_primary_node() {
    local upstream_node
    local upstream_id
    local upstream_host
    local upstream_port
    local primary_host=""
    local primary_port="$REDIS_PORT"

    readarray -t upstream_node < <(redis_sentinel_get_upstream_node)
    upstream_id=${upstream_node[0]}
    upstream_host=${upstream_node[1]}
    upstream_port=${upstream_node[2]:-$REDIS_PORT}
    [[ -n "$upstream_host" ]] && info "Auto-detected primary node: '${upstream_host}:${upstream_port}'"

    if [[ "$REDIS_HEAD_NODE" = true ]]; then
        if [[ -z "$upstream_host" ]] \
          || [[ "${upstream_host}" = "$REDIS_NODE_IP" ]] \
          || [[ "${upstream_host}" = "$REDIS_NODE_HOST" ]]; then
            info "Starting Redis normally for the head..."
        else
            info "Current primary is '${upstream_host}:${upstream_port}'. Starting as replica following it..."
            primary_host="$upstream_host"
            primary_port="$upstream_port"
        fi
    else
        if [[ -z "$upstream_host" ]]; then
            info "Can not find primary. Starting as replica following the head..."
            # for workers, it there is no primary found, use head
            # TODO: shall we start as primary since there is no primary
            primary_host="$REDIS_HEAD_HOST"
            primary_port="$REDIS_PORT"
        elif [[ "${upstream_host}" = "$REDIS_NODE_IP" ]] \
          || [[ "${upstream_host}" = "$REDIS_NODE_HOST" ]]; then
            # myself marked as primary. It seemed that primary failover is not happening
            info "Failover didn't happen. Starting as primary..."
        else
            info "Starting as secondary following '${upstream_host}:${upstream_port}'..."
            primary_host="$upstream_host"
            primary_port="$upstream_port"
        fi
    fi

    [[ -n "$primary_host" ]] && debug "Primary node: '${primary_host}:${primary_port}'"
    echo "$primary_host"
    echo "$primary_port"
}

########################
# Generates env vars for the node
# Globals:
#   REDIS_SENTINEL_*
# Arguments:
#   None
# Returns:
#   Series of exports used for initialization
#########################
redis_sentinel_set_role() {
    local role="secondary"
    local primary_node
    local primary_host
    local primary_port

    readarray -t primary_node < <(redis_sentinel_get_primary_node)
    primary_host=${primary_node[0]}
    primary_port=${primary_node[1]:-$REDIS_PORT}

    if [[ -z "$primary_host" ]]; then
      info "There are no nodes with primary role. Assuming the primary role..."
      primary_host="$REDIS_NODE_HOST"
      primary_port="$REDIS_PORT"
      role="primary"
    else
      info "Node configured as secondary"
      role="secondary"
    fi

    export REDIS_REPLICATION_ROLE="$role"
    export REDIS_PRIMARY_HOST="$primary_host"
    export REDIS_PRIMARY_PORT="$primary_port"
}

########################
# Configure general options of sentinel
# Globals:
#   REDIS_*
# Arguments:
#   None
# Returns:
#   None
#########################
redis_sentinel_configure_default() {
    if [[ -n "$REDIS_PASSWORD" ]]; then
        redis_sentinel_conf_set requirepass "$REDIS_PASSWORD"
    else
        redis_sentinel_conf_unset requirepass
        # Allow remote connections without password
        redis_sentinel_conf_set protected-mode no
    fi
    if [[ "$REDIS_NODE_IP" != "$REDIS_NODE_HOST" ]]; then
        redis_sentinel_conf_set "sentinel resolve-hostnames" "yes"
        redis_sentinel_conf_set "sentinel announce-hostnames" "yes"
    else
        redis_sentinel_conf_set "sentinel resolve-hostnames" "no"
        redis_sentinel_conf_set "sentinel announce-hostnames" "no"
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
redis_sentinel_configure_master() {
    # Master set
    # shellcheck disable=SC2153
    local primary_port=${REDIS_PRIMARY_PORT:-$REDIS_PORT}
    redis_sentinel_conf_set "sentinel monitor" "${REDIS_SENTINEL_MASTER_NAME} ${REDIS_PRIMARY_HOST} ${primary_port} ${REDIS_SENTINEL_QUORUM}"

    if [[ -n "$REDIS_PASSWORD" ]]; then
        redis_sentinel_conf_set "sentinel auth-pass" "${REDIS_SENTINEL_MASTER_NAME} $REDIS_PASSWORD"
    fi
}

########################
# Initialize and generate sentinel conf file
# Globals:
#   REDIS_SENTINEL_*
# Arguments:
#   None
# Returns:
#   None
#########################
redis_sentinel_initialize() {
    redis_sentinel_configure_default
    redis_sentinel_configure_master
}
