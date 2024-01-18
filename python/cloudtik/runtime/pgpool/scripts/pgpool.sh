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
