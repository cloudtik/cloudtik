#!/usr/bin/env bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

# Load generic functions
. "$ROOT_DIR"/common/scripts/util-log.sh
. "$ROOT_DIR"/common/scripts/util-file.sh
. "$ROOT_DIR"/common/scripts/util-fs.sh

########################
# Return PostgreSQL major version
# Globals:
#   POSTGRES_*
# Arguments:
#   None
# Returns:
#   String
#########################
postgres_get_major_version() {
    psql --version | grep -oE "[0-9]+\.[0-9]+" | grep -oE "^[0-9]+"
}

########################
# Change a PostgreSQL configuration file by setting a property
# Globals:
#   POSTGRES_CONF_FILE
# Arguments:
#   $1 - property
#   $2 - value
#   $3 - Path to configuration file (default: $POSTGRES_CONF_FILE)
# Returns:
#   None
#########################
postgres_set_property() {
    local -r property="${1:?missing property}"
    local -r value="${2:?missing value}"
    local -r conf_file="${3:-$POSTGRES_CONF_FILE}"
    local psql_conf
    if grep -qE "^#*\s*${property}" "$conf_file" >/dev/null; then
        replace_in_file "$conf_file" "^#*\s*${property}\s*=.*" "${property} = '${value}'" false
    else
        echo "${property} = '${value}'" >>"$conf_file"
    fi
}

# usage: postgres_file_env VAR [DEFAULT]
#    ie: postgres_file_env 'XYZ_DB_PASSWORD' 'example'
# (will allow for "$XYZ_DB_PASSWORD_FILE" to fill in the value of
#  "$XYZ_DB_PASSWORD" from a file)
postgres_file_env() {
	local var="$1"
	local fileVar="${var}_FILE"
	local def="${2:-}"
	if [ "${!var:-}" ] && [ "${!fileVar:-}" ]; then
		printf >&2 'error: both %s and %s are set (but are exclusive)\n' "$var" "$fileVar"
		exit 1
	fi
	local val="$def"
	if [ "${!var:-}" ]; then
		val="${!var}"
	elif [ "${!fileVar:-}" ]; then
		val="$(< "${!fileVar}")"
	fi
	export "$var"="$val"
	unset "$fileVar"
}

# used to create initial postgres directories and if run as root, ensure ownership to the "postgres" user
postgres_create_db_directories() {
	local user; user="$(id -u)"

	mkdir -p "$PGDATA"
	# ignore failure since there are cases where we can't chmod (and PostgreSQL might fail later anyhow - it's picky about permissions of this directory)
	chmod 00700 "$PGDATA" || :

	# ignore failure since it will be fine when using the image provided directory; see also https://github.com/docker-library/postgres/pull/289
	mkdir -p /var/run/postgresql || :
	chmod 03775 /var/run/postgresql || :

	# Create the transaction log directory before initdb is run so the directory is owned by the correct user
	if [ -n "${POSTGRES_INITDB_WALDIR:-}" ]; then
		mkdir -p "$POSTGRES_INITDB_WALDIR"
		if [ "$user" = '0' ]; then
			find "$POSTGRES_INITDB_WALDIR" \! -user postgres -exec chown postgres '{}' +
		fi
		chmod 700 "$POSTGRES_INITDB_WALDIR"
	fi

	# allow the container to be started with `--user`
	if [ "$user" = '0' ]; then
		find "$PGDATA" \! -user postgres -exec chown postgres '{}' +
		find /var/run/postgresql \! -user postgres -exec chown postgres '{}' +
	fi
}

# initialize empty PGDATA directory with new database via 'initdb'
# arguments to `initdb` can be passed via POSTGRES_INITDB_ARGS or as arguments to this function
# `initdb` automatically creates the "postgres", "template0", and "template1" dbnames
# this is also where the database user is created, specified by `POSTGRES_USER` env
postgres_init_database_dir() {
	# "initdb" is particular about the current user existing in "/etc/passwd", so we use "nss_wrapper" to fake that if necessary
	# see https://github.com/docker-library/postgres/pull/253, https://github.com/docker-library/postgres/issues/359, https://cwrap.org/nss_wrapper.html
	local uid; uid="$(id -u)"
	if ! getent passwd "$uid" &> /dev/null; then
		# see if we can find a suitable "libnss_wrapper.so" (https://salsa.debian.org/sssd-team/nss-wrapper/-/commit/b9925a653a54e24d09d9b498a2d913729f7abb15)
		local wrapper
		for wrapper in {/usr,}/lib{/*,}/libnss_wrapper.so; do
			if [ -s "$wrapper" ]; then
				NSS_WRAPPER_PASSWD="$(mktemp)"
				NSS_WRAPPER_GROUP="$(mktemp)"
				export LD_PRELOAD="$wrapper" NSS_WRAPPER_PASSWD NSS_WRAPPER_GROUP
				local gid; gid="$(id -g)"
				printf 'postgres:x:%s:%s:PostgreSQL:%s:/bin/false\n' "$uid" "$gid" "$PGDATA" > "$NSS_WRAPPER_PASSWD"
				printf 'postgres:x:%s:\n' "$gid" > "$NSS_WRAPPER_GROUP"
				break
			fi
		done
	fi

	if [ -n "${POSTGRES_INITDB_WALDIR:-}" ]; then
		set -- --waldir "$POSTGRES_INITDB_WALDIR" "$@"
	fi

	# --pwfile refuses to handle a properly-empty file (hence the "\n"): https://github.com/docker-library/postgres/issues/1025
	eval 'initdb --username="$POSTGRES_USER" --pwfile=<(printf "%s\n" "$POSTGRES_PASSWORD") '"$POSTGRES_INITDB_ARGS"' "$@"'

	# unset/cleanup "nss_wrapper" bits
	if [[ "${LD_PRELOAD:-}" == */libnss_wrapper.so ]]; then
		rm -f "$NSS_WRAPPER_PASSWD" "$NSS_WRAPPER_GROUP"
		unset LD_PRELOAD NSS_WRAPPER_PASSWD NSS_WRAPPER_GROUP
	fi
}

# print large warning if POSTGRES_PASSWORD is long
# error if both POSTGRES_PASSWORD is empty and POSTGRES_HOST_AUTH_METHOD is not 'trust'
# print large warning if POSTGRES_HOST_AUTH_METHOD is set to 'trust'
# assumes database is not set up, ie: [ -z "$DATABASE_ALREADY_EXISTS" ]
postgres_verify_minimum_env() {
	# check password first so we can output the warning before postgres
	# messes it up
	if [ "${#POSTGRES_PASSWORD}" -ge 100 ]; then
		cat >&2 <<-'EOWARN'

			WARNING: The supplied POSTGRES_PASSWORD is 100+ characters.

			  This will not work if used via PGPASSWORD with "psql".

			  https://www.postgresql.org/message-id/flat/E1Rqxp2-0004Qt-PL%40wrigleys.postgresql.org (BUG #6412)
			  https://github.com/docker-library/postgres/issues/507

		EOWARN
	fi
	if [ -z "$POSTGRES_PASSWORD" ] && [ 'trust' != "$POSTGRES_HOST_AUTH_METHOD" ]; then
		# The - option suppresses leading tabs but *not* spaces. :)
		cat >&2 <<-'EOE'
			Error: Database is uninitialized and superuser password is not specified.
			       You must specify POSTGRES_PASSWORD to a non-empty value for the
			       superuser.

			       You may also use "POSTGRES_HOST_AUTH_METHOD=trust" to allow all
			       connections without a password. This is *not* recommended.

			       See PostgreSQL documentation about "trust":
			       https://www.postgresql.org/docs/current/auth-trust.html
		EOE
		exit 1
	fi
	if [ 'trust' = "$POSTGRES_HOST_AUTH_METHOD" ]; then
		cat >&2 <<-'EOWARN'
			********************************************************************************
			WARNING: POSTGRES_HOST_AUTH_METHOD has been set to "trust". This will allow
			         anyone with access to the Postgres port to access your database without
			         a password, even if POSTGRES_PASSWORD is set. See PostgreSQL
			         documentation about "trust":
			         https://www.postgresql.org/docs/current/auth-trust.html

			         It is not recommended to use POSTGRES_HOST_AUTH_METHOD=trust. Replace
			         it with "POSTGRES_PASSWORD=password" instead.
			********************************************************************************
		EOWARN
	fi
}

postgres_init_db_and_user() {
	if [ -n "$POSTGRES_DATABASE_NAME" ]; then
		postgres_process_sql <<<"CREATE DATABASE ${POSTGRES_DATABASE_NAME};"
	fi
	if [ -n "$POSTGRES_DATABASE_USER" ] && [ -n "$POSTGRES_DATABASE_PASSWORD" ]; then
		postgres_process_sql <<<"CREATE USER $POSTGRES_DATABASE_USER WITH PASSWORD '$POSTGRES_DATABASE_PASSWORD';"
		if [ -n "$POSTGRES_DATABASE_NAME" ]; then
			postgres_process_sql <<<"GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_DATABASE_NAME} TO $POSTGRES_DATABASE_NAME;"
		fi
	fi
}

postgres_setup_replication_user() {
	POSTGRES_REPLICATION_PASSWORD="${POSTGRES_REPLICATION_PASSWORD:-cloudtik}"
	postgres_process_sql <<<"CREATE ROLE $POSTGRES_REPLICATION_USER WITH REPLICATION LOGIN PASSWORD '$POSTGRES_REPLICATION_PASSWORD';"
}

# usage: postgres_process_init_files [file [file [...]]]
#    ie: postgres_process_init_files /always-initdb.d/*
# process initializer files, based on file extensions and permissions
postgres_process_init_files() {
	# psql here for backwards compatibility "${psql[@]}"
	psql=( postgres_process_sql )

	printf '\n'
	local f
	for f; do
		case "$f" in
			*.sh)
				# https://github.com/docker-library/postgres/issues/450#issuecomment-393167936
				# https://github.com/docker-library/postgres/pull/452
				if [ -x "$f" ]; then
					printf '%s: running %s\n' "$0" "$f"
					"$f"
				else
					printf '%s: sourcing %s\n' "$0" "$f"
					. "$f"
				fi
				;;
			*.sql)     printf '%s: running %s\n' "$0" "$f"; postgres_process_sql -f "$f"; printf '\n' ;;
			*.sql.gz)  printf '%s: running %s\n' "$0" "$f"; gunzip -c "$f" | postgres_process_sql; printf '\n' ;;
			*.sql.xz)  printf '%s: running %s\n' "$0" "$f"; xzcat "$f" | postgres_process_sql; printf '\n' ;;
			*.sql.zst) printf '%s: running %s\n' "$0" "$f"; zstd -dc "$f" | postgres_process_sql; printf '\n' ;;
			*)         printf '%s: ignoring %s\n' "$0" "$f" ;;
		esac
		printf '\n'
	done
}

# Execute sql script, passed via stdin (or -f flag of pqsl)
# usage: postgres_process_sql [psql-cli-args]
#    ie: postgres_process_sql --dbname=mydb <<<'INSERT ...'
#    ie: postgres_process_sql -f my-file.sql
#    ie: postgres_process_sql <my-file.sql
postgres_process_sql() {
	local query_runner=( psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --no-password --no-psqlrc )
	if [ -n "$POSTGRES_DB" ]; then
		query_runner+=( --dbname "$POSTGRES_DB" )
	fi

	PGHOST= PGHOSTADDR= "${query_runner[@]}" "$@"
}

# create initial database
# uses environment variables for input: POSTGRES_DB
postgres_setup_db() {
	local dbAlreadyExists
	dbAlreadyExists="$(
		POSTGRES_DB= postgres_process_sql --dbname postgres --set db="$POSTGRES_DB" --tuples-only <<-'EOSQL'
			SELECT 1 FROM pg_database WHERE datname = :'db' ;
		EOSQL
	)"
	if [ -z "$dbAlreadyExists" ]; then
		POSTGRES_DB= postgres_process_sql --dbname postgres --set db="$POSTGRES_DB" <<-'EOSQL'
			CREATE DATABASE :"db" ;
		EOSQL
		printf '\n'
	fi
}

# Loads various settings that are used elsewhere in the script
# This should be called before any other functions
postgres_setup_env() {
	postgres_file_env 'POSTGRES_PASSWORD'

	postgres_file_env 'POSTGRES_USER' 'postgres'
	postgres_file_env 'POSTGRES_DB' "$POSTGRES_USER"
	postgres_file_env 'POSTGRES_DATABASE_NAME'
	postgres_file_env 'POSTGRES_DATABASE_USER'
	postgres_file_env 'POSTGRES_DATABASE_PASSWORD'
	postgres_file_env 'POSTGRES_INITDB_ARGS'
	: "${POSTGRES_HOST_AUTH_METHOD:=}"

	declare -g DATABASE_ALREADY_EXISTS
	# look specifically for PG_VERSION, as it is expected in the DB dir
	if [ -s "$PGDATA/PG_VERSION" ]; then
		DATABASE_ALREADY_EXISTS='true'
	fi
}

# append POSTGRES_HOST_AUTH_METHOD to pg_hba.conf for "host" connections
# all arguments will be passed along as arguments to `postgres` for getting the value of 'password_encryption'
pg_setup_hba_conf() {
	# default authentication method is md5 on versions before 14
	# https://www.postgresql.org/about/news/postgresql-14-released-2318/
	if [ "$1" = 'postgres' ]; then
		shift
	fi
	local auth
	# check the default/configured encryption and use that as the auth method
	auth="$(postgres -C password_encryption "$@")"
	: "${POSTGRES_HOST_AUTH_METHOD:=$auth}"
	{
		printf '\n'
		if [ 'trust' = "$POSTGRES_HOST_AUTH_METHOD" ]; then
			printf '# warning trust is enabled for all connections\n'
			printf '# see https://www.postgresql.org/docs/12/auth-trust.html\n'
		fi
		printf 'host all all all %s\n' "$POSTGRES_HOST_AUTH_METHOD"
		printf 'host replication %s all %s\n' "${POSTGRES_REPLICATION_USER}" "$POSTGRES_HOST_AUTH_METHOD"
	} >> "$PGDATA/pg_hba.conf"
}

# start socket-only postgresql server for setting up or running scripts
# all arguments will be passed along as arguments to `postgres` (via pg_ctl)
postgres_temp_server_start() {
	if [ "$1" = 'postgres' ]; then
		shift
	fi

	# internal start of server in order to allow setup using psql client
	# does not listen on external TCP/IP and waits until start finishes
	set -- "$@" -c listen_addresses='' -p "${PGPORT:-5432}"

	PGUSER="${PGUSER:-$POSTGRES_USER}" \
	pg_ctl -D "$PGDATA" \
		-o "$(printf '%q ' "$@")" \
		-w start
}

# stop postgresql server after done setting up user and running scripts
postgres_temp_server_stop() {
	PGUSER="${PGUSER:-postgres}" \
	pg_ctl -D "$PGDATA" -m fast -w stop
}

# check arguments for an option that would cause postgres to stop
# return true if there is one
_pg_want_help() {
	local arg
	for arg; do
		case "$arg" in
			# postgres --help | grep 'then exit'
			# leaving out -C on purpose since it always fails and is unhelpful:
			# postgres: could not access the server configuration file "/var/lib/postgresql/data/postgresql.conf": No such file or directory
			-'?'|--help|--describe-config|-V|--version)
				return 0
				;;
		esac
	done
	return 1
}

postgres_set_synchronous_standby_names() {
    local synchronous_standby_names=""
    local standby_names=""
    # WARNING: this depends on the head node seq id = 1
    # all the nodes including the primary are listed
    END_SERVER_ID=$((POSTGRES_SYNCHRONOUS_SIZE+1))
    for i in $(seq 1 $END_SERVER_ID); do
        if [ -z "$standby_names" ]; then
            standby_names="postgres_$i"
        else
            standby_names="$standby_names,postgres_$i"
        fi
    done

    if [ "${POSTGRES_SYNCHRONOUS_MODE}" == "first" ]; then
        synchronous_standby_names="FIRST ${POSTGRES_SYNCHRONOUS_NUM} (${standby_names})"
    elif [ "${POSTGRES_SYNCHRONOUS_MODE}" == "any" ]; then
        synchronous_standby_names="ANY ${POSTGRES_SYNCHRONOUS_NUM} (${standby_names})"
    else
        synchronous_standby_names="${standby_names}"
    fi

    postgres_set_property "synchronous_standby_names" "$synchronous_standby_names"
    postgres_set_property "synchronous_commit" "$POSTGRES_SYNCHRONOUS_COMMIT_MODE"
}

postgres_setup_synchronous_standby(){
  if [ "${POSTGRES_CLUSTER_MODE}" == "replication" ] \
      && [ "${POSTGRES_SYNCHRONOUS_MODE}" != "none" ]; then
      postgres_set_synchronous_standby_names
  fi
}

########################
# Execute an arbitrary query/queries against the running PostgreSQL service and print the output
# Stdin:
#   Query/queries to execute
# Globals:
#   POSTGRES_*
# Arguments:
#   $1 - Database where to run the queries
#   $2 - User to run queries
#   $3 - Password
#   $4 - Extra options (eg. -tA)
# Returns:
#   None
#########################
postgres_execute() {
    local -r db="${1:-}"
    local -r user="${2:-postgres}"
    local -r pass="${3:-}"
    local opts
    read -r -a opts <<<"${@:4}"

    local args=("-U" "$user" "-p" "${POSTGRES_PORT:-5432}")
    [[ -n "$db" ]] && args+=("-d" "$db")
    [[ "${#opts[@]}" -gt 0 ]] && args+=("${opts[@]}")

    # Execute the Query/queries from stdin
    PGPASSWORD=$pass psql "${args[@]}"
}

########################
# Execute an arbitrary query/queries against the running PostgreSQL service
# Stdin:
#   Query/queries to execute
# Globals:
#   POSTGRES_*
# Arguments:
#   $1 - Database where to run the queries
#   $2 - User to run queries
#   $3 - Password
#   $4 - Extra options (eg. -tA)
# Returns:
#   None
#########################
postgres_execute_ex() {
    if [[ "${POSTGRES_QUITE:-false}" = true ]]; then
        "postgres_execute" "$@" >/dev/null 2>&1
    elif [[ "${POSTGRES_NO_ERRORS:-false}" = true ]]; then
        "postgres_execute" "$@" 2>/dev/null
    else
        "postgres_execute" "$@"
    fi
}

########################
# Execute an arbitrary query/queries against a remote PostgreSQL service and print to stdout
# Stdin:
#   Query/queries to execute
# Globals:
#   POSTGRES_*
# Arguments:
#   $1 - Remote PostgreSQL service hostname
#   $2 - Remote PostgreSQL service port
#   $3 - Database where to run the queries
#   $4 - User to run queries
#   $5 - Password
#   $6 - Extra options (eg. -tA)
# Returns:
#   None
postgres_remote_execute() {
    local -r hostname="${1:?hostname is required}"
    local -r port="${2:?port is required}"
    local -a args=("-h" "$hostname" "-p" "$port")
    shift 2
    "postgres_execute" "$@" "${args[@]}"
}

########################
# Execute an arbitrary query/queries against a remote PostgreSQL service
# Stdin:
#   Query/queries to execute
# Globals:
#   POSTGRES_*
# Arguments:
#   $1 - Remote PostgreSQL service hostname
#   $2 - Remote PostgreSQL service port
#   $3 - Database where to run the queries
#   $4 - User to run queries
#   $5 - Password
#   $6 - Extra options (eg. -tA)
# Returns:
#   None
postgres_remote_execute_ex() {
    if [[ "${POSTGRES_QUITE:-false}" = true ]]; then
        "postgres_remote_execute" "$@" >/dev/null 2>&1
    elif [[ "${POSTGRES_NO_ERRORS:-false}" = true ]]; then
        "postgres_remote_execute" "$@" 2>/dev/null
    else
        "postgres_remote_execute" "$@"
    fi
}

########################
# Retrieves the WAL directory in use by PostgreSQL / to use if not initialized yet
# Globals:
#   POSTGRES_*
# Arguments:
#   None
# Returns:
#   the path to the WAL directory, or empty if not set
#########################
postgres_get_waldir() {
    if [[ -L "$PGDATA/pg_wal" && -d "$PGDATA/pg_wal" ]]; then
        readlink -f "$PGDATA/pg_wal"
    else
        # Uninitialized - using value from $POSTGRES_INITDB_WAL_DIR if set
        echo "$POSTGRES_INITDB_WAL_DIR"
    fi
}

#########################
postgres_configure_recovery() {
    info "Setting up streaming replication slave..."
    local -r replication_user="${1:-${POSTGRES_REPLICATION_USER}}"
    local -r escaped_password="${POSTGRES_REPLICATION_PASSWORD//\&/\\&}"
    local -r password_str="${2:-"password=${escaped_password}"}"
    local -r application_name="${POSTGRES_APP_NAME:-"${POSTGRES_SERVER_NAME}"}"
    postgres_set_property "primary_conninfo" \
      "host=${POSTGRES_PRIMARY_HOST} port=${POSTGRES_PRIMARY_PORT} user=${replication_user} application_name=${application_name} ${password_str}" \
      "$POSTGRES_CONF_FILE"
    touch "$PGDATA"/standby.signal
}

postgres_clone_primary() {
    # for replica, we needs to do a pg_basebackup from master
    # Cannot use an emtpy data directory or a data directory initialized
    # by initdb (this method will make the data files with different identifier.
    # This process will setup primary_conninfo in the postgres.auto.conf
    # and the standby.signal in the data directory
    # TODO: flag to choose whether need to do this aggressively
    if [ -s "$PGDATA/PG_VERSION" ]; then
        info "Deleting existing data in $PGDATA..."
        rm -rf "$PGDATA"
        # setup data directories and permissions
        postgres_create_db_directories
    fi

    export PGPASSWORD="${POSTGRES_REPLICATION_PASSWORD:-cloudtik}"
    local replication_slot_options=""
    if [ ! -z "$POSTGRES_REPLICATION_SLOT_NAME" ]; then
      replication_slot_options="-C -S $POSTGRES_REPLICATION_SLOT_NAME"
    fi
    pg_basebackup -h ${POSTGRES_PRIMARY_HOST} \
      -U ${POSTGRES_REPLICATION_USER} --no-password ${replication_slot_options} \
      -X stream -R -D $PGDATA
    unset PGPASSWORD
}
