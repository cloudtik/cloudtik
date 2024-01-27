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

# postgres functions
. "$BIN_DIR"/postgres.sh

########################
# Get repmgr password method
# Globals:
#   POSTGRES_REPMGR_*
# Arguments:
#   None
# Returns:
#   String
#########################
repmgr_get_env_password() {
    if [[ "$POSTGRES_REPMGR_USE_PASSFILE" = "true" ]]; then
        echo "PGPASSFILE=${POSTGRES_REPMGR_PASSFILE_PATH}"
    else
        echo "PGPASSWORD=${POSTGRES_REPMGR_PASSWORD}"
    fi
}

########################
# Get repmgr conninfo password method
# Globals:
#   POSTGRES_REPMGR_*
# Arguments:
#   None
# Returns:
#   String
#########################
repmgr_get_conninfo_password() {
    if [[ "$POSTGRES_REPMGR_USE_PASSFILE" = "true" ]]; then
        echo "passfile=${POSTGRES_REPMGR_PASSFILE_PATH}"
    else
        echo "password=${POSTGRES_REPMGR_PASSWORD}"
    fi
}

########################
# Get primary conninfo password method
# Globals:
#   POSTGRES_REPMGR_*
# Arguments:
#   None
# Returns:
#   String
#########################
repmgr_get_primary_conninfo_password() {
    if [[ "$POSTGRES_REPMGR_USE_PASSFILE" = "true" ]]; then
        echo "passfile=${POSTGRES_REPMGR_PASSFILE_PATH}"
    else
        local -r escaped_password="${POSTGRES_REPLICATION_PASSWORD//\&/\\&}"
        echo "password=${escaped_password}"
    fi
}

#######################
# Generate password file if necessary
# Globals:
#   POSTGRES_REPMGR_*
# Arguments:
#   None
# Returns:
#   None
#########################
repmgr_generate_password_file() {
    if [[ "$POSTGRES_REPMGR_USE_PASSFILE" = "true" ]]; then
        if [[ -f "${POSTGRES_REPMGR_PASSFILE_PATH}" ]]; then
            rm -f "${POSTGRES_REPMGR_PASSFILE_PATH}"
        fi
        local -r replication_user="${POSTGRES_REPLICATION_USER}"
        echo "*:*:*:${replication_user}:${POSTGRES_REPLICATION_PASSWORD}" >"${POSTGRES_REPMGR_PASSFILE_PATH}"
        echo "*:*:*:${POSTGRES_REPMGR_USER}:${POSTGRES_REPMGR_PASSWORD}" >>"${POSTGRES_REPMGR_PASSFILE_PATH}"
        chmod 600 "${POSTGRES_REPMGR_PASSFILE_PATH}"
    fi
}

########################
# Change a Repmgr configuration file by setting a property
# Globals:
#   POSTGRES_REPMGR_*
# Arguments:
#   $1 - property
#   $2 - value
#   $3 - Path to configuration file (default: $POSTGRES_REPMGR_CONF_FILE)
# Returns:
#   None
#########################
repmgr_set_property() {
    local -r property="${1:?missing property}"
    local -r value="${2:-}"
    local -r conf_file="${3:-$POSTGRES_REPMGR_CONF_FILE}"

    replace_in_file "$conf_file" "^#*\s*${property}\s*=.*" "${property} = '${value}'" false
}

########################
# Configure repmgr preload libraries
# Globals:
#   POSTGRES_*
# Arguments:
#   None
# Returns:
#   None
#########################
repmgr_configure_preload() {
    if [[ -n "$POSTGRES_SHARED_PRELOAD_LIBRARIES" ]]; then
        if [[ "$POSTGRES_SHARED_PRELOAD_LIBRARIES" =~ ^(repmgr|REPMGR)$ ]]; then
            postgres_set_property "shared_preload_libraries" "$POSTGRES_SHARED_PRELOAD_LIBRARIES"
        else
            postgres_set_property "shared_preload_libraries" "repmgr, ${POSTGRES_SHARED_PRELOAD_LIBRARIES}"
        fi
    else
        postgres_set_property "shared_preload_libraries" "repmgr"
    fi
}

repmgr_setup_hba_conf() {
	if [ "$1" = 'postgres' ]; then
		shift
	fi
	local auth
	# check the default/configured encryption and use that as the auth method
	auth="$(postgres -C password_encryption "$@")"
	: "${POSTGRES_HOST_AUTH_METHOD:=$auth}"
	{
		printf 'host all %s all %s\n' "$POSTGRES_REPMGR_USER" "$POSTGRES_HOST_AUTH_METHOD"
		printf 'host replication %s all %s\n' "$POSTGRES_REPMGR_USER" "$POSTGRES_HOST_AUTH_METHOD"
		printf 'host %s %s all %s\n' "$POSTGRES_REPMGR_DATABASE" "$POSTGRES_REPMGR_USER" "$POSTGRES_HOST_AUTH_METHOD"
	} >> "$PGDATA/pg_hba.conf"
}

########################
# Create the repmgr user (with )
# Globals:
#   POSTGRES_REPMGR_*
#   POSTGRES_*
# Arguments:
#   None
# Returns:
#   None
#########################
repmgr_create_repmgr_user() {
    local -r escaped_password="${POSTGRES_REPMGR_PASSWORD//\'/\'\'}"
    info "Creating repmgr user: $POSTGRES_REPMGR_USER"

    # The repmgr user is created as superuser for simplicity
    postgres_process_sql <<<"CREATE ROLE \"${POSTGRES_REPMGR_USER}\" WITH LOGIN CREATEDB PASSWORD '${escaped_password}';"
    postgres_process_sql <<<"ALTER USER ${POSTGRES_REPMGR_USER} WITH SUPERUSER;"
    # set the repmgr user's search path to include the 'repmgr' schema name
    postgres_process_sql <<<"ALTER USER ${POSTGRES_REPMGR_USER} SET search_path TO repmgr, \"\$user\", public;"
}

########################
# Creates the repmgr database
# Globals:
#   POSTGRES_REPMGR_*
#   POSTGRES_*
# Arguments:
#   None
# Returns:
#   None
#########################
repmgr_create_repmgr_db() {
    info "Creating repmgr database: $POSTGRES_REPMGR_DATABASE"
    postgres_process_sql <<<"CREATE DATABASE $POSTGRES_REPMGR_DATABASE;"
}

########################
# Ask seed nodes which node is the primary
# Globals:
#   POSTGRES_REPMGR_*
# Arguments:
#   Non
# Returns:
#   String[] - (host port)
#########################
repmgr_get_upstream_node() {
    local primary_conninfo
    local pretending_primary_id=""
    local pretending_primary_host=""
    local pretending_primary_port=""
    local host=""
    local port=""
    local suggested_primary_id=""
    local suggested_primary_host=""
    local suggested_primary_port=""

    if [[ -n "$POSTGRES_REPMGR_SEED_NODES" ]]; then
        info "Querying all seed nodes for common upstream node..."
        read -r -a nodes <<<"$(tr ',;' ' ' <<<"${POSTGRES_REPMGR_SEED_NODES}")"
        for node in "${nodes[@]}"; do
            host="$node"
            port="$POSTGRES_PORT"
            debug "Checking node '$host:$port'..."
            local query="SELECT node_id, conninfo FROM repmgr.show_nodes WHERE (upstream_node_name IS NULL OR upstream_node_name = '') AND active=true"
            if ! primary_conninfo="$(echo "$query" | POSTGRES_NO_ERRORS=true postgres_remote_execute_ex "$host" "$port" "$POSTGRES_REPMGR_DATABASE" "$POSTGRES_REPMGR_USER" "$POSTGRES_REPMGR_PASSWORD" "-tA")"; then
                debug "Skipping: failed to get primary from the node '$host:$port'!"
                continue
            elif [[ -z "$primary_conninfo" ]]; then
                debug "Skipping: failed to get information about primary nodes!"
                continue
            elif [[ "$(echo "$primary_conninfo" | wc -l)" -eq 1 ]]; then
                suggested_primary_id="$(echo "$primary_conninfo" | awk -F '|' '{print $1}')"
                suggested_primary_host="$(echo "$primary_conninfo" | awk -F 'host=' '{print $2}' | awk '{print $1}')"
                suggested_primary_port="$(echo "$primary_conninfo" | awk -F 'port=' '{print $2}' | awk '{print $1}')"
                debug "Pretending primary role node - '${suggested_primary_id}'"
                if [[ -n "$pretending_primary_id" ]]; then
                    if [[ "${pretending_primary_id}" != "${suggested_primary_id}" ]]; then
                        warn "Conflict of pretending primary role nodes (previously: '${pretending_primary_id}', now: '${suggested_primary_id}')"
                        pretending_primary_id="" && pretending_primary_host="" && pretending_primary_port="" && break
                    fi
                else
                    debug "Pretending primary set to '${suggested_primary_id}'!"
                    pretending_primary_id="$suggested_primary_id"
                    pretending_primary_host="$suggested_primary_host"
                    pretending_primary_port="$suggested_primary_port"
                fi
            else
                warn "There were more than one primary when getting primary from node '$host:$port'"
                pretending_primary_id="" && pretending_primary_host="" && pretending_primary_port="" && break
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
#   POSTGRES_REPMGR_*
# Arguments:
#   None
# Returns:
#   String[] - (host port)
#########################
repmgr_get_primary_node() {
    local upstream_node
    local upstream_id
    local upstream_host
    local upstream_port
    local primary_host=""
    local primary_port="$POSTGRES_PORT"

    readarray -t upstream_node < <(repmgr_get_upstream_node)
    upstream_id=${upstream_node[0]}
    upstream_host=${upstream_node[1]}
    upstream_port=${upstream_node[2]:-$POSTGRES_PORT}
    [[ -n "$upstream_host" ]] && info "Auto-detected primary node: '${upstream_host}:${upstream_port}'"

    if [[ "$POSTGRES_HEAD_NODE" = true ]]; then
        if [[ -z "$upstream_host" ]] || [[ "${upstream_id}" = "$POSTGRES_REPMGR_NODE_ID" ]]; then
            info "Starting PostgreSQL normally for the head..."
        else
            info "Current primary is '${upstream_host}:${upstream_port}'. Cloning/rewinding it and acting as a standby node..."
            primary_host="$upstream_host"
            primary_port="$upstream_port"
        fi
    else
        if [[ -z "$upstream_host" ]]; then
            info "Can not find primary. Starting as standby following the head..."
            # for workers, it there is no primary found, use head
            # TODO: shall we start as primary since there is no primary
            primary_host="$POSTGRES_HEAD_HOST"
            primary_port="$POSTGRES_PORT"
        elif [[ "${upstream_id}" = "$POSTGRES_REPMGR_NODE_ID" ]]; then
            # myself marked as primary. It seemed that primary failover is not happening
            info "Failover didn't happen. Starting as primary..."
        else
            info "Starting as standby following '${upstream_host}:${upstream_port}'..."
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
#   POSTGRES_REPMGR_*
# Arguments:
#   None
# Returns:
#   Series of exports to be used as 'eval' arguments
#########################
repmgr_set_role() {
    local role="standby"
    local primary_node
    local primary_host
    local primary_port

    readarray -t primary_node < <(repmgr_get_primary_node)
    primary_host=${primary_node[0]}
    primary_port=${primary_node[1]:-$POSTGRES_PORT}

    if [[ -z "$primary_host" ]]; then
      info "There are no nodes with primary role. Assuming the primary role..."
      role="primary"
    else
      info "Node configured as standby"
      role="standby"
    fi

    export POSTGRES_ROLE="$role"
    export POSTGRES_PRIMARY_HOST="$primary_host"
    export POSTGRES_PRIMARY_PORT="$primary_port"
}

########################
# Waits until the primary node responds
# Globals:
#   POSTGRES_REPMGR_*
# Arguments:
#   None
# Returns:
#   None
#########################
repmgr_wait_primary_node() {
    local return_value=1
    local -i timeout=60
    local -i step=10
    local -i max_tries=$((timeout / step))
    local schemata
    info "Waiting for primary node..."
    debug "Wait for schema $POSTGRES_REPMGR_DATABASE.repmgr on '${POSTGRES_PRIMARY_HOST}:${POSTGRES_PRIMARY_PORT}', will try $max_tries times with $step delay seconds (TIMEOUT=$timeout)"
    for ((i = 0; i <= timeout; i += step)); do
        local query="SELECT 1 FROM information_schema.schemata WHERE catalog_name='$POSTGRES_REPMGR_DATABASE' AND schema_name='repmgr'"
        if ! schemata="$(echo "$query" | POSTGRES_NO_ERRORS=true postgres_remote_execute_ex "$POSTGRES_PRIMARY_HOST" "$POSTGRES_PRIMARY_PORT" "$POSTGRES_REPMGR_DATABASE" "$POSTGRES_REPMGR_USER" "$POSTGRES_REPMGR_PASSWORD" "-tA")"; then
            debug "Host '${POSTGRES_PRIMARY_HOST}:${POSTGRES_PRIMARY_PORT}' is not accessible"
        else
            if [[ $schemata -ne 1 ]]; then
                debug "Schema $POSTGRES_REPMGR_DATABASE.repmgr is still not accessible"
            else
                debug "Schema $POSTGRES_REPMGR_DATABASE.repmgr exists!"
                return_value=0 && break
            fi
        fi
        sleep "$step"
    done
    return $return_value
}

########################
# Clones data from primary node
# Globals:
#   POSTGRES_REPMGR_*
#   POSTGRES_*
# Arguments:
#   None
# Returns:
#   None
#########################
repmgr_clone_primary() {
    # Clears WAL directory if existing (pg_basebackup requires the WAL dir to be empty)
    local -r waldir=$(postgres_get_waldir)
    if [[ -d "$waldir" ]]; then
        info "Deleting existing WAL directory $waldir..."
        rm -rf "$waldir" && ensure_dir_exists "$waldir"
    fi

    info "Cloning data from primary node..."
    local -r flags=("-f" "$POSTGRES_REPMGR_CONF_FILE" "-h" "$POSTGRES_PRIMARY_HOST" "-p" "$POSTGRES_PRIMARY_PORT" "-U" "$POSTGRES_REPMGR_USER" "-d" "$POSTGRES_REPMGR_DATABASE" "-D" "$PGDATA" "standby" "clone" "--fast-checkpoint" "--force")

    if [[ "$POSTGRES_REPMGR_USE_PASSFILE" = "true" ]]; then
        PGPASSFILE="$POSTGRES_REPMGR_PASSFILE_PATH" execute_command "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}"
    else
        PGPASSWORD="$POSTGRES_REPMGR_PASSWORD" execute_command "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}"
    fi
}

########################
# Execute pg_rewind to get data from the primary node
# Globals:
#   POSTGRES_REPMGR_*
# Arguments:
#   None
# Returns:
#   None
#########################
repmgr_pgrewind() {
    info "Running pg_rewind data to primary node..."
    local -r flags=("-D" "$PGDATA" "--source-server" "host=${POSTGRES_PRIMARY_HOST} port=${POSTGRES_PRIMARY_PORT} user=${POSTGRES_REPMGR_USER} dbname=${POSTGRES_REPMGR_DATABASE}")

    if [[ "$POSTGRES_REPMGR_USE_PASSFILE" = "true" ]]; then
        PGPASSFILE="$POSTGRES_REPMGR_PASSFILE_PATH" execute_command "pg_rewind" "${flags[@]}"
    else
        PGPASSWORD="$POSTGRES_REPMGR_PASSWORD" execute_command "pg_rewind" "${flags[@]}"
    fi
}

########################
# Rejoin node
# Globals:
#   POSTGRES_REPMGR_*
# Arguments:
#   None
# Returns:
#   None
#########################
repmgr_rewind() {
    info "Rejoining node..."

    ensure_dir_exists "$PGDATA"
    if is_boolean_yes "$POSTGRES_REPMGR_USE_PGREWIND" \
        && [[ "${POSTGRES_REPLICATION_SLOT}" != "true" ]]; then
        info "Using pg_rewind to primary node..."
        if ! repmgr_pgrewind; then
            warn "pg_rewind failed, resorting to data cloning"
            repmgr_clone_primary
        else
            info "Successfully pg_rewind to primary node."
            # pg_backup use generate recovery config while pg_rewind will not
            local -r password_str=$(repmgr_get_primary_conninfo_password)
            postgres_configure_recovery "${POSTGRES_REPLICATION_USER}" "$password_str"
        fi
    else
        repmgr_clone_primary
    fi
}

########################
# Register a node as primary
# Globals:
#   POSTGRES_REPMGR_*
# Arguments:
#   None
# Returns:
#   None
#########################
repmgr_register_primary() {
    info "Registering Primary..."
    local -r flags=("-f" "$POSTGRES_REPMGR_CONF_FILE" "master" "register" "--force")

    execute_command "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}"
}

########################
# Resgister a node as standby
# Globals:
#   POSTGRES_REPMGR_*
# Arguments:
#   None
# Returns:
#   None
#########################
repmgr_register_standby() {
    info "Registering Standby node..."
    local -r flags=("standby" "register" "-f" "$POSTGRES_REPMGR_CONF_FILE" "--force" "--verbose")

    execute_command "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}"
}

########################
# Unregister standby node
# Globals:
#   POSTGRES_REPMGR_*
# Arguments:
#   None
# Returns:
#   None
#########################
repmgr_unregister_standby() {
    info "Unregistering standby node..."

    local -r flags=("standby" "unregister" "-f" "$POSTGRES_REPMGR_CONF_FILE" "--node-id=$POSTGRES_REPMGR_NODE_ID")

    # The command below can fail when the node doesn't exist yet
    execute_command "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}" || true
}


########################
# Register witness
# Globals:
#   POSTGRES_REPMGR_*
# Arguments:
#   None
# Returns:
#   None
#########################
repmgr_register_witness() {
    info "Registering witness node..."
    local -r flags=("-f" "$POSTGRES_REPMGR_CONF_FILE" "witness" "register" "-h" "$POSTGRES_PRIMARY_HOST" "--force" "--verbose")

    repmgr_wait_primary_node

    if [[ "$POSTGRES_REPMGR_USE_PASSFILE" = "true" ]]; then
        PGPASSFILE="$POSTGRES_REPMGR_PASSFILE_PATH" execute_command "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}"
    else
        PGPASSWORD="$POSTGRES_REPMGR_PASSWORD" execute_command "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}"
    fi
}

########################
# Unregister witness
# Globals:
#   POSTGRES_REPMGR_*
# Arguments:
#   None
# Returns:
#   None
#########################
repmgr_unregister_witness() {
    info "Unregistering witness node..."
    local -r flags=("-f" "$POSTGRES_REPMGR_CONF_FILE" "witness" "unregister" "-h" "$POSTGRES_PRIMARY_HOST" "--verbose")

    # The command below can fail when the node doesn't exist yet
    if [[ "$POSTGRES_REPMGR_USE_PASSFILE" = "true" ]]; then
        PGPASSFILE="$POSTGRES_REPMGR_PASSFILE_PATH" execute_command "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}" || true
    else
        PGPASSWORD="$POSTGRES_REPMGR_PASSWORD" execute_command "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}" || true
    fi
}

########################
# Standby follow.
# Globals:
#   POSTGRES_REPMGR_*
# Arguments:
#   None
# Returns:
#   None
#########################
repmgr_standby_follow() {
    info "Running standby follow..."
    local -r flags=("standby" "follow" "-f" "$POSTGRES_REPMGR_CONF_FILE" "-W" "--log-level" "DEBUG" "--verbose")

    if [[ "$POSTGRES_REPMGR_USE_PASSFILE" = "true" ]]; then
        PGPASSFILE="$POSTGRES_REPMGR_PASSFILE_PATH" execute_command "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}"
    else
        PGPASSWORD="$POSTGRES_REPMGR_PASSWORD" execute_command "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}"
    fi
}

########################
# Check if regmgrd is running
# Globals:
#   POSTGRES_HOME
# Arguments:
#   $1 - pid file
# Returns:
#   Boolean
#########################
is_regmgrd_running() {
    local pid_file="${1:-"${POSTGRES_HOME}/repmgrd.pid"}"
    local pid
    pid="$(get_pid_from_file "$pid_file")"

    if [[ -z "$pid" ]]; then
        false
    else
        is_service_running "$pid"
    fi
}

########################
# Check if regmgrd is not running
# Globals:
#   POSTGRES_HOME
# Arguments:
#   $1 - pid file
# Returns:
#   Boolean
#########################
is_regmgrd_not_running() {
    ! is_regmgrd_running "$@"
}
