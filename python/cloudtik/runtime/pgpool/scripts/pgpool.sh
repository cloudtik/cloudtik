#!/usr/bin/env bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

# Load generic functions
. "$ROOT_DIR"/common/scripts/util-log.sh
. "$ROOT_DIR"/common/scripts/util-file.sh
. "$ROOT_DIR"/common/scripts/util-fs.sh
. "$ROOT_DIR"/common/scripts/util-os.sh
. "$ROOT_DIR"/common/scripts/util-value.sh

########################
# Modify the pgpool.conf file by setting a property
# Globals:
#   PGPOOL_*
# Arguments:
#   $1 - property
#   $2 - value
#   $3 - Path to configuration file (default: $PGPOOL_CONF_FILE)
# Returns:
#   None
#########################
pgpool_set_property() {
    local -r property="${1:?missing property}"
    local -r value="${2:-}"
    local -r conf_file="${3:-$PGPOOL_CONF_FILE}"
    replace_in_file "$conf_file" "^#*\s*${property}\s*=.*" "${property} = '${value}'" false
}

########################
# Generates a password file for local authentication
# Globals:
#   PGPOOL_*
# Arguments:
#   None
# Returns:
#   None
#########################
pgpool_generate_password_file() {
    info "Generating password file for local authentication..."

    local -a password_encryption_cmd=("pg_md5")

    if [[ "$PGPOOL_AUTHENTICATION_METHOD" = "scram-sha-256" ]]; then
        if is_file_writable "$PGPOOLKEYFILE"; then
            # Creating a PGPOOLKEYFILE as it is writeable
            local -r aes_key="${PGPOOL_AES_KEY:-$(head -c 20 /dev/urandom | base64)}"
            echo "$aes_key" > "$PGPOOLKEYFILE"
            # Fix permissions for PGPOOLKEYFILE
            chmod 0600 "$PGPOOLKEYFILE"
        fi
        password_encryption_cmd=("pg_enc" "--key-file=${PGPOOLKEYFILE}")
    fi

    execute_command "${password_encryption_cmd[@]}" \
      -m --config-file="$PGPOOL_CONF_FILE" -u "$PGPOOL_POSTGRES_USER" "$PGPOOL_POSTGRES_PASSWORD"

    if [[ -n "${PGPOOL_POSTGRES_CUSTOM_USERS}" ]]; then
        read -r -a custom_users_list <<<"$(tr ',;' ' ' <<<"${PGPOOL_POSTGRES_CUSTOM_USERS}")"
        read -r -a custom_passwords_list <<<"$(tr ',;' ' ' <<<"${PGPOOL_POSTGRES_CUSTOM_PASSWORDS}")"

        local index=0
        for user in "${custom_users_list[@]}"; do
            execute_command "${password_encryption_cmd[@]}" \
              -m --config-file="$PGPOOL_CONF_FILE" -u "$user" "${custom_passwords_list[$index]}"
            ((index += 1))
        done
    fi
}

########################
# Generate a password file for pgpool admin user
# Globals:
#   PGPOOL_*
# Arguments:
#   None
# Returns:
#   None
#########################
pgpool_generate_admin_password_file() {
    info "Generating password file for pgpool admin user..."
    local passwd

    passwd=$(pg_md5 "$PGPOOL_ADMIN_PASSWORD")
    cat >>"$PGPOOL_PCP_FILE" <<EOF
$PGPOOL_ADMIN_USER:$passwd
EOF
}

########################
# Create basic pg_hba.conf file
# Globals:
#   PGPOOL_*
# Arguments:
#   None
# Returns:
#   None
#########################
pgpool_create_pghba() {
    local all_authentication="$PGPOOL_AUTHENTICATION_METHOD"
    local postgres_authentication="$all_authentication"
    is_boolean_yes "$PGPOOL_ENABLE_LDAP" && all_authentication="pam pamservice=pgpool"
    local postgres_auth_line=""
    local sr_check_auth_line=""
    info "Generating pg_hba.conf file..."

    postgres_auth_line="host     all             ${PGPOOL_POSTGRES_USER}       all         ${postgres_authentication}"
    if [[ -n "$PGPOOL_REPLICATION_USER" ]]; then
        sr_check_auth_line="host     all             ${PGPOOL_REPLICATION_USER}       all         trust"
    fi

    if ! is_empty_value "$PGPOOL_TLS_CA_FILE"; then
        cat >>"$PGPOOL_HBA_FILE" <<EOF
hostssl     all             all             0.0.0.0/0               cert
hostssl     all             all             ::/0                    cert
EOF
    fi

    cat >>"$PGPOOL_HBA_FILE" <<EOF
${sr_check_auth_line}
${postgres_auth_line}
host     all             all                all         ${all_authentication}
EOF
}

########################
# Attach offline backend node
# Globals:
#   PGPOOL_*
# Arguments:
#   node_id
# Returns:
#   None
#########################
pgpool_attach_node() {
    local -r node_id=${1:?node id is missing}

    info "Attaching backend node:${node_id}..."
    PCPPASSFILE=$(mktemp /tmp/pcppass-XXXXX)
    export PCPPASSFILE
    echo "localhost:${PGPOOL_PCP_PORT}:${PGPOOL_ADMIN_USER}:${PGPOOL_ADMIN_PASSWORD}" >"${PCPPASSFILE}"
    pcp_attach_node -h localhost -U "${PGPOOL_ADMIN_USER}" -p ${PGPOOL_PCP_PORT} -n "${node_id}" -w
    rm -rf "${PCPPASSFILE}"
}

########################
# Attach offline backend node
# Globals:
#   PGPOOL_*
# Arguments:
#   node string
# Returns:
#   None
#########################
pgpool_check_and_attach_node() {
    local -r node=${1:?node is missing}
    IFS="|" read -ra node_info <<< "$node"
    local node_id="${node_info[0]}"
    local node_host="${node_info[1]}"
    local node_port="${node_info[2]}"
    if [[ $(PGCONNECT_TIMEOUT=3 PGPASSWORD="${PGPOOL_POSTGRES_PASSWORD}" psql -U "${PGPOOL_POSTGRES_USER}" \
        -d postgres -h "${node_host}" -p "${node_port}" -tA -c "SELECT 1" || true) == 1 ]]; then
        # attach backend if it has come back online
        pgpool_attach_node "${node_id}"
    fi
}

########################
# Check pgpool health and attached offline backends when they are online
# Globals:
#   PGPOOL_*
# Arguments:
#   None
# Returns:
#   0 when healthy
#   1 when unhealthy
#########################
pgpool_healthcheck() {
    info "Checking pgpool health..."
    local backends
    # Timeout should be in sync with liveness probe timeout and number of nodes which could be down together
    # Only nodes marked UP in pgpool are tested. Each failed standby backend consumes up to PGPOOL_CONNECT_TIMEOUT
    # to test. Network split is worst-case scenario.
    # NOTE: command blocks indefinitely if primary node is marked UP in pgpool but is DOWN in reality and connection
    # times-out. Example is again network-split.
    PGPOOL_HEALTH_CHECK_PSQL_TIMEOUT="${PGPOOL_HEALTH_CHECK_PSQL_TIMEOUT:-15}"
    backends="$(PGCONNECT_TIMEOUT=$PGPOOL_HEALTH_CHECK_PSQL_TIMEOUT PGPASSWORD="$PGPOOL_POSTGRES_PASSWORD" \
        psql -U "$PGPOOL_POSTGRES_USER" -d postgres -h localhost -p "$PGPOOL_PORT" \
        -tA -c "SHOW pool_nodes;")" || backends="command failed"
    if [[ "$backends" != "command failed" ]]; then
        # look up backends that are marked offline and being up - attach only status=down and pg_status=up
        # situation down|down means PG is not yet ready to be attached
        for node in $(echo "${backends}" | grep "down|up" | tr -d ' '); do
            pgpool_check_and_attach_node "${node}"
        done
        for node in $(echo "${backends}" | grep "unused|up" | tr -d ' '); do
            pgpool_check_and_attach_node "${node}"
        done
    else
        # backends command failed
        return 1
    fi
}
