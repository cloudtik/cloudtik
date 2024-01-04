#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

# Load generic functions
. "$ROOT_DIR"/common/scripts/util-file.sh
. "$ROOT_DIR"/common/scripts/util-log.sh
. "$ROOT_DIR"/common/scripts/util-os.sh

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
            postgresql_set_property "shared_preload_libraries" "$POSTGRES_SHARED_PRELOAD_LIBRARIES"
        else
            postgresql_set_property "shared_preload_libraries" "repmgr, ${POSTGRES_SHARED_PRELOAD_LIBRARIES}"
        fi
    else
        postgresql_set_property "shared_preload_libraries" "repmgr"
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
    debug "Wait for schema $POSTGRES_REPMGR_DATABASE.repmgr on '${POSTGRES_REPMGR_CURRENT_PRIMARY_HOST}:${POSTGRES_REPMGR_CURRENT_PRIMARY_PORT}', will try $max_tries times with $step delay seconds (TIMEOUT=$timeout)"
    for ((i = 0; i <= timeout; i += step)); do
        local query="SELECT 1 FROM information_schema.schemata WHERE catalog_name='$POSTGRES_REPMGR_DATABASE' AND schema_name='repmgr'"
        if ! schemata="$(echo "$query" | POSTGRES_NO_ERRORS=true postgresql_remote_execute_ex "$POSTGRES_REPMGR_CURRENT_PRIMARY_HOST" "$POSTGRES_REPMGR_CURRENT_PRIMARY_PORT" "$POSTGRES_REPMGR_DATABASE" "$POSTGRES_REPMGR_USER" "$POSTGRES_REPMGR_PASSWORD" "-tA")"; then
            debug "Host '${POSTGRES_REPMGR_CURRENT_PRIMARY_HOST}:${POSTGRES_REPMGR_CURRENT_PRIMARY_PORT}' is not accessible"
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
    local -r waldir=$(postgresql_get_waldir)
    if [[ -d "$waldir" ]]; then
        info "Deleting existing WAL directory $waldir..."
        rm -rf "$waldir" && ensure_dir_exists "$waldir"
    fi

    info "Cloning data from primary node..."
    local -r flags=("-f" "$POSTGRES_REPMGR_CONF_FILE" "-h" "$POSTGRES_REPMGR_CURRENT_PRIMARY_HOST" "-p" "$POSTGRES_REPMGR_CURRENT_PRIMARY_PORT" "-U" "$POSTGRES_REPMGR_USER" "-d" "$POSTGRES_REPMGR_DATABASE" "-D" "$POSTGRES_DATA_DIR" "standby" "clone" "--fast-checkpoint" "--force")

    if [[ "$POSTGRES_REPMGR_USE_PASSFILE" = "true" ]]; then
        PGPASSFILE="$POSTGRES_REPMGR_PASSFILE_PATH" debug_execute "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}"
    else
        PGPASSWORD="$POSTGRES_REPMGR_PASSWORD" debug_execute "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}"
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
    local -r flags=("-D" "$POSTGRES_DATA_DIR" "--source-server" "host=${POSTGRES_REPMGR_CURRENT_PRIMARY_HOST} port=${POSTGRES_REPMGR_CURRENT_PRIMARY_PORT} user=${POSTGRES_REPMGR_USER} dbname=${POSTGRES_REPMGR_DATABASE}")

    if [[ "$POSTGRES_REPMGR_USE_PASSFILE" = "true" ]]; then
        PGPASSFILE="$POSTGRES_REPMGR_PASSFILE_PATH" debug_execute "${POSTGRES_BIN_DIR}/pg_rewind" "${flags[@]}"
    else
        PGPASSWORD="$POSTGRES_REPMGR_PASSWORD" debug_execute "${POSTGRES_BIN_DIR}/pg_rewind" "${flags[@]}"
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

    ensure_dir_exists "$POSTGRES_DATA_DIR"
    if is_boolean_yes "$POSTGRES_REPMGR_USE_PGREWIND"; then
        info "Using pg_rewind to primary node..."
        if ! repmgr_pgrewind; then
            warn "pg_rewind failed, resorting to data cloning"
            repmgr_clone_primary
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

    debug_execute "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}"
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

    debug_execute "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}"
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
    debug_execute "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}" || true
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
    local -r flags=("-f" "$POSTGRES_REPMGR_CONF_FILE" "witness" "register" "-h" "$POSTGRES_REPMGR_CURRENT_PRIMARY_HOST" "--force" "--verbose")

    repmgr_wait_primary_node

    if [[ "$POSTGRES_REPMGR_USE_PASSFILE" = "true" ]]; then
        PGPASSFILE="$POSTGRES_REPMGR_PASSFILE_PATH" debug_execute "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}"
    else
        PGPASSWORD="$POSTGRES_REPMGR_PASSWORD" debug_execute "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}"
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
    local -r flags=("-f" "$POSTGRES_REPMGR_CONF_FILE" "witness" "unregister" "-h" "$POSTGRES_REPMGR_CURRENT_PRIMARY_HOST" "--verbose")

    # The command below can fail when the node doesn't exist yet
    if [[ "$POSTGRES_REPMGR_USE_PASSFILE" = "true" ]]; then
        PGPASSFILE="$POSTGRES_REPMGR_PASSFILE_PATH" debug_execute "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}" || true
    else
        PGPASSWORD="$POSTGRES_REPMGR_PASSWORD" debug_execute "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}" || true
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
        PGPASSFILE="$POSTGRES_REPMGR_PASSFILE_PATH" debug_execute "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}"
    else
        PGPASSWORD="$POSTGRES_REPMGR_PASSWORD" debug_execute "${POSTGRES_REPMGR_BIN_DIR}/repmgr" "${flags[@]}"
    fi
}
