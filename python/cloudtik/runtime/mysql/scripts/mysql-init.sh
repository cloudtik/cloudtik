#!/bin/bash
set -eo pipefail
shopt -s nullglob

# logging functions
mysql_log() {
	local type="$1"; shift
	# accept argument string or stdin
	local text="$*"; if [ "$#" -eq 0 ]; then text="$(cat)"; fi
	local dt; dt="$(date --rfc-3339=seconds)"
	printf '%s [%s] [init]: %s\n' "$dt" "$type" "$text"
}
mysql_note() {
	mysql_log Note "$@"
}
mysql_warn() {
	mysql_log Warn "$@" >&2
}
mysql_error() {
	mysql_log ERROR "$@" >&2
	exit 1
}

# usage: file_env VAR [DEFAULT]
#    ie: file_env 'XYZ_DB_PASSWORD' 'example'
# (will allow for "$XYZ_DB_PASSWORD_FILE" to fill in the value of
#  "$XYZ_DB_PASSWORD" from a file)
file_env() {
	local var="$1"
	local fileVar="${var}_FILE"
	local def="${2:-}"
	if [ "${!var:-}" ] && [ "${!fileVar:-}" ]; then
		mysql_error "Both $var and $fileVar are set (but are exclusive)"
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

# check to see if this file is being run or sourced from another script
_is_sourced() {
	# https://unix.stackexchange.com/a/215279
	[ "${#FUNCNAME[@]}" -ge 2 ] \
		&& [ "${FUNCNAME[0]}" = '_is_sourced' ] \
		&& [ "${FUNCNAME[1]}" = 'source' ]
}

# usage: mysql_process_init_files [file [file [...]]]
#    ie: mysql_process_init_files /always-initdb.d/*
# process initializer files, based on file extensions
mysql_process_init_files() {
	# mysql here for backwards compatibility "${mysql[@]}"
	mysql=( mysql_process_sql )

	echo
	local f
	for f; do
		case "$f" in
			*.sh)
				# https://github.com/docker-library/postgres/issues/450#issuecomment-393167936
				# https://github.com/docker-library/postgres/pull/452
				if [ -x "$f" ]; then
					mysql_note "$0: running $f"
					"$f"
				else
					mysql_note "$0: sourcing $f"
					. "$f"
				fi
				;;
			*.sql)     mysql_note "$0: running $f"; mysql_process_sql < "$f"; echo ;;
			*.sql.bz2) mysql_note "$0: running $f"; bunzip2 -c "$f" | mysql_process_sql; echo ;;
			*.sql.gz)  mysql_note "$0: running $f"; gunzip -c "$f" | mysql_process_sql; echo ;;
			*.sql.xz)  mysql_note "$0: running $f"; xzcat "$f" | mysql_process_sql; echo ;;
			*.sql.zst) mysql_note "$0: running $f"; zstd -dc "$f" | mysql_process_sql; echo ;;
			*)         mysql_warn "$0: ignoring $f" ;;
		esac
		echo
	done
}

# arguments necessary to run "mysqld --verbose --help" successfully (used for testing configuration validity and for extracting default/configured values)
_verboseHelpArgs=(
	--verbose --help
	--log-bin-index="$(mktemp -u)" # https://github.com/docker-library/mysql/issues/136
)

mysql_check_config() {
	local toRun=( "$@" "${_verboseHelpArgs[@]}" ) errors
	if ! errors="$("${toRun[@]}" 2>&1 >/dev/null)"; then
		mysql_error $'mysqld failed while attempting to check config\n\tcommand was: '"${toRun[*]}"$'\n\t'"$errors"
	fi
}

# Fetch value from server config
# We use mysqld --verbose --help instead of my_print_defaults because the
# latter only show values present in config files, and not server defaults
mysql_get_config() {
	local conf="$1"; shift
	"$@" "${_verboseHelpArgs[@]}" 2>/dev/null \
		| awk -v conf="$conf" '$1 == conf && /^[^ \t]/ { sub(/^[^ \t]+[ \t]+/, ""); print; exit }'
	# match "datadir      /some/path with/spaces in/it here" but not "--xyz=abc\n     datadir (xyz)"
}

# Ensure that the package default socket can also be used
# since rpm packages are compiled with a different socket location
# and "mysqlsh --mysql" doesn't read the [client] config
# related to https://github.com/docker-library/mysql/issues/829
mysql_socket_fix() {
	local defaultSocket
	defaultSocket="$(mysql_get_config 'socket' mysqld --no-defaults)"
	if [ "$defaultSocket" != "$SOCKET" ]; then
		ln -sfTv "$SOCKET" "$defaultSocket" || :
	fi
}

# Do a temporary startup of the MySQL server, for init purposes
mysql_temp_server_start() {
  # For 5.7+ the server is ready for use as soon as startup command unblocks
  if ! "$@" --daemonize --skip-networking --default-time-zone=SYSTEM --socket="${SOCKET}"; then
    mysql_error "Unable to start server."
  fi
}

# Stop the server. When using a local socket file mysqladmin will block until
# the shutdown is complete.
mysql_temp_server_stop() {
	if ! mysqladmin --defaults-extra-file=<( _mysql_passfile ) shutdown -uroot --socket="${SOCKET}"; then
		mysql_error "Unable to shut down server."
	fi
}

# Verify that the minimally required password settings are set for new databases.
mysql_verify_minimum_env() {
	if [ -z "$MYSQL_ROOT_PASSWORD" -a -z "$MYSQL_ALLOW_EMPTY_PASSWORD" -a -z "$MYSQL_RANDOM_ROOT_PASSWORD" ]; then
		mysql_error <<-'EOF'
			Database is uninitialized and password option is not specified
			    You need to specify one of the following as an environment variable:
			    - MYSQL_ROOT_PASSWORD
			    - MYSQL_ALLOW_EMPTY_PASSWORD
			    - MYSQL_RANDOM_ROOT_PASSWORD
		EOF
	fi

	# This will prevent the CREATE USER from failing (and thus exiting with a half-initialized database)
	if [ "$MYSQL_USER" = 'root' ]; then
		mysql_error <<-'EOF'
			MYSQL_USER="root", MYSQL_USER and MYSQL_PASSWORD are for configuring a regular user and cannot be used for the root user
			    Remove MYSQL_USER="root" and use one of the following to control the root user password:
			    - MYSQL_ROOT_PASSWORD
			    - MYSQL_ALLOW_EMPTY_PASSWORD
			    - MYSQL_RANDOM_ROOT_PASSWORD
		EOF
	fi

	# warn when missing one of MYSQL_USER or MYSQL_PASSWORD
	if [ -n "$MYSQL_USER" ] && [ -z "$MYSQL_PASSWORD" ]; then
		mysql_warn 'MYSQL_USER specified, but missing MYSQL_PASSWORD; MYSQL_USER will not be created'
	elif [ -z "$MYSQL_USER" ] && [ -n "$MYSQL_PASSWORD" ]; then
		mysql_warn 'MYSQL_PASSWORD specified, but missing MYSQL_USER; MYSQL_PASSWORD will be ignored'
	fi
}

# creates folders for the database
# also ensures permission for mysql user of run as root
mysql_create_db_directories() {
	local user; user="$(id -u)"

	local -A dirs=( ["$DATADIR"]=1 )
	local dir
	dir="$(dirname "$SOCKET")"
	dirs["$dir"]=1

	# "datadir" and "socket" are already handled above (since they were already queried previously)
	local conf
	for conf in \
		general-log-file \
		keyring_file_data \
		pid-file \
		secure-file-priv \
		slow-query-log-file \
	; do
		dir="$(mysql_get_config "$conf" "$@")"

		# skip empty values
		if [ -z "$dir" ] || [ "$dir" = 'NULL' ]; then
			continue
		fi
		case "$conf" in
			secure-file-priv)
				# already points at a directory
				;;
			*)
				# other config options point at a file, but we need the directory
				dir="$(dirname "$dir")"
				;;
		esac

		dirs["$dir"]=1
	done

	mkdir -p "${!dirs[@]}"

	if [ "$user" = "0" ]; then
		# this will cause less disk access than `chown -R`
		find "${!dirs[@]}" \! -user mysql -exec chown --no-dereference mysql '{}' +
	fi
}

# initializes the database directory
mysql_init_database_dir() {
	if [ "$MYSQL_INIT_WITH_CMD_OPTIONS" = true ]; then
		mysql_note "Initializing database files with cmd options"
		mysqld --initialize-insecure --default-time-zone=SYSTEM --datadir=$DATADIR
	else
	  mysql_note "Initializing database files"
		"$@" --initialize-insecure --default-time-zone=SYSTEM
	fi
	mysql_note "Database files initialized"
}

# Loads various settings that are used elsewhere in the script
# This should be called after mysql_check_config, but before any other functions
mysql_setup_env() {
	# Get config
	declare -g DATADIR SOCKET
	DATADIR="$(mysql_get_config 'datadir' "$@")"
	SOCKET="$(mysql_get_config 'socket' "$@")"

	# Initialize values that might be stored in a file
	file_env 'MYSQL_ROOT_HOST' '%'
	file_env 'MYSQL_DATABASE'
	file_env 'MYSQL_USER'
	file_env 'MYSQL_PASSWORD'
	file_env 'MYSQL_ROOT_PASSWORD'

	declare -g DATABASE_ALREADY_EXISTS
	if [ -d "$DATADIR/mysql" ]; then
		DATABASE_ALREADY_EXISTS='true'
	fi
}

# Execute sql script, passed via stdin
# usage: mysql_process_sql [--dont-use-mysql-root-password] [mysql-cli-args]
#    ie: mysql_process_sql --database=mydb <<<'INSERT ...'
#    ie: mysql_process_sql --dont-use-mysql-root-password --database=mydb <my-file.sql
mysql_process_sql() {
	passfileArgs=()
	if [ '--dont-use-mysql-root-password' = "$1" ]; then
		passfileArgs+=( "$1" )
		shift
	fi
	# args sent in can override this db, since they will be later in the command
	if [ -n "$MYSQL_DATABASE" ]; then
		set -- --database="$MYSQL_DATABASE" "$@"
	fi

	mysql --defaults-extra-file=<( _mysql_passfile "${passfileArgs[@]}") --protocol=socket -uroot -hlocalhost --socket="${SOCKET}" --comments "$@"
}

# Execute sql script passed via stdin
# usage: mysql_run_sql [mysql-cli-args]
#    ie: mysql_run_sql --database=mydb <<<'INSERT ...'
#    ie: mysql_run_sql --database=mydb <my-file.sql
mysql_run_sql() {
	passfileArgs=()
	# args sent in can override this db, since they will be later in the command
	if [ -n "$MYSQL_DATABASE" ]; then
		set -- --database="$MYSQL_DATABASE" "$@"
	fi

	mysql --defaults-extra-file=<( _mysql_passfile "${passfileArgs[@]}") -uroot --comments "$@"
}

# Initializes database with timezone info and root password
mysql_setup_db() {
	# Load timezone info into database
	if [ -z "$MYSQL_INITDB_SKIP_TZINFO" ]; then
		# sed is for https://bugs.mysql.com/bug.php?id=20545
		mysql_tzinfo_to_sql /usr/share/zoneinfo \
			| sed 's/Local time zone must be set--see zic manual page/FCTY/' \
			| mysql_process_sql --dont-use-mysql-root-password --database=mysql
			# tell mysql_process_sql to not use MYSQL_ROOT_PASSWORD since it is not set yet
	fi
	# Generate random root password
	if [ -n "$MYSQL_RANDOM_ROOT_PASSWORD" ]; then
		MYSQL_ROOT_PASSWORD="$(openssl rand -base64 24)"; export MYSQL_ROOT_PASSWORD
		mysql_note "GENERATED ROOT PASSWORD: $MYSQL_ROOT_PASSWORD"
	fi
	# Sets root password and creates root users for non-localhost hosts
	local rootCreate=
	# default root to listen for connections from anywhere
	if [ -n "$MYSQL_ROOT_HOST" ] && [ "$MYSQL_ROOT_HOST" != 'localhost' ]; then
		# no, we don't care if read finds a terminating character in this heredoc
		# https://unix.stackexchange.com/questions/265149/why-is-set-o-errexit-breaking-this-read-heredoc-expression/265151#265151
		read -r -d '' rootCreate <<-EOSQL || true
			CREATE USER 'root'@'${MYSQL_ROOT_HOST}' IDENTIFIED BY '${MYSQL_ROOT_PASSWORD}' ;
			GRANT ALL ON *.* TO 'root'@'${MYSQL_ROOT_HOST}' WITH GRANT OPTION ;
		EOSQL
	fi

	local passwordSet=
	# no, we don't care if read finds a terminating character in this heredoc (see above)
	read -r -d '' passwordSet <<-EOSQL || true
		ALTER USER 'root'@'localhost' IDENTIFIED BY '${MYSQL_ROOT_PASSWORD}' ;
	EOSQL

	# tell mysql_process_sql to not use MYSQL_ROOT_PASSWORD since it is just now being set
	mysql_process_sql --dont-use-mysql-root-password --database=mysql <<-EOSQL
		-- What's done in this file shouldn't be replicated
		--  or products like mysql-fabric won't work
		SET @@SESSION.SQL_LOG_BIN=0;

		${passwordSet}
		GRANT ALL ON *.* TO 'root'@'localhost' WITH GRANT OPTION ;
		FLUSH PRIVILEGES ;
		${rootCreate}
		DROP DATABASE IF EXISTS test ;
	EOSQL
}

mysql_setup_user_db() {
	# Creates a custom database and user if specified
	if [ -n "$MYSQL_DATABASE" ]; then
		mysql_note "Creating database ${MYSQL_DATABASE}"
		mysql_process_sql --database=mysql <<<"CREATE DATABASE IF NOT EXISTS \`$MYSQL_DATABASE\` ;"
	fi

	if [ -n "$MYSQL_USER" ] && [ -n "$MYSQL_PASSWORD" ]; then
		mysql_note "Creating user ${MYSQL_USER}"
		mysql_process_sql --database=mysql <<<"CREATE USER '$MYSQL_USER'@'%' IDENTIFIED BY '$MYSQL_PASSWORD' ;"

		if [ -n "$MYSQL_DATABASE" ]; then
			mysql_note "Giving user ${MYSQL_USER} access to schema ${MYSQL_DATABASE}"
			mysql_process_sql --database=mysql <<<"GRANT ALL ON \`${MYSQL_DATABASE//_/\\_}\`.* TO '$MYSQL_USER'@'%' ;"
		fi
	fi
}

mysql_setup_replication_user() {
  mysql_note "Setting up replication user: repl_user"
  mysql_process_sql --database=mysql <<-EOSQL
		-- What's done in this file shouldn't be replicated
		--  or products like mysql-fabric won't work
		SET @@SESSION.SQL_LOG_BIN=0;

		CREATE USER 'repl_user'@'%' IDENTIFIED BY 'cloudtik';
		GRANT REPLICATION SLAVE ON *.* TO 'repl_user'@'%';
		FLUSH PRIVILEGES ;
	EOSQL
}

mysql_setup_group_replication_user() {
  mysql_note "Setting up group replication user: repl_user"
  mysql_process_sql --database=mysql <<-EOSQL
		-- What's done in this file shouldn't be replicated
		--  or products like mysql-fabric won't work
		SET @@SESSION.SQL_LOG_BIN=0;

		CREATE USER 'repl_user'@'%' IDENTIFIED BY 'cloudtik';
		GRANT REPLICATION SLAVE ON *.* TO 'repl_user'@'%';
		GRANT CONNECTION_ADMIN ON *.* TO 'repl_user'@'%';
		GRANT BACKUP_ADMIN ON *.* TO 'repl_user'@'%';
		GRANT GROUP_REPLICATION_STREAM ON *.* TO 'repl_user'@'%';
		FLUSH PRIVILEGES ;
	EOSQL
}

mysql_setup_replication() {
  mysql_note "Setting up replication source to ${MYSQL_REPLICATION_SOURCE_HOST}"
  mysql_process_sql --database=mysql <<-EOSQL
		CHANGE REPLICATION SOURCE TO
		    SOURCE_HOST = '${MYSQL_REPLICATION_SOURCE_HOST}',
		    SOURCE_USER = 'repl_user',
		    SOURCE_PASSWORD = 'cloudtik',
		    SOURCE_AUTO_POSITION = 1,
		    GET_SOURCE_PUBLIC_KEY = 1;
	EOSQL
}

mysql_setup_group_replication() {
  mysql_note "Setting up group replication"
  mysql_process_sql --database=mysql <<-EOSQL
		CHANGE REPLICATION SOURCE TO
		    SOURCE_USER = 'repl_user',
		    SOURCE_PASSWORD = 'cloudtik',
		    FOR CHANNEL 'group_replication_recovery';
	EOSQL
}

mysql_bootstrap_group_replication() {
  mysql_note "Starting group replication with bootstrap"
  mysql_run_sql --database=mysql "$@" <<-EOSQL
		 SET GLOBAL group_replication_bootstrap_group=ON;
		 START GROUP_REPLICATION;
		 SET GLOBAL group_replication_bootstrap_group=OFF;
	EOSQL
}

mysql_start_group_replication() {
  mysql_note "Starting group replication"
  mysql_run_sql --database=mysql "$@" <<-EOSQL
		 START GROUP_REPLICATION;
	EOSQL
}

mysql_check_connection() {
	for i in $(seq 1 10); do
		[ $i -gt 1 ] && echo "Waiting for service ready..." && sleep 1;
		mysql_run_sql --database=mysql "$@" <<<'\q' && s=0 && break || s=$?;
	done;
	return $s
}

_mysql_passfile() {
	# echo the password to the "file" the client uses
	# the client command will use process substitution to create a file on the fly
	# ie: --defaults-extra-file=<( _mysql_passfile )
	if [ '--dont-use-mysql-root-password' != "$1" ] && [ -n "$MYSQL_ROOT_PASSWORD" ]; then
		cat <<-EOF
			[client]
			password="${MYSQL_ROOT_PASSWORD}"
		EOF
	fi
}

# Mark root user as expired so the password must be changed before anything
# else can be done (only supported for 5.6+)
mysql_expire_root_user() {
	if [ -n "$MYSQL_ONETIME_PASSWORD" ]; then
		mysql_process_sql --database=mysql <<-EOSQL
			ALTER USER 'root'@'%' PASSWORD EXPIRE;
		EOSQL
	fi
}

# check arguments for an option that would cause mysqld to stop
# return true if there is one
_mysql_want_help() {
	local arg
	for arg; do
		case "$arg" in
			-'?'|--help|--print-defaults|-V|--version)
				return 0
				;;
		esac
	done
	return 1
}

_main() {
	# if command starts with an option, prepend mysqld
	if [ "${1:0:1}" = '-' ]; then
		set -- mysqld "$@"
	fi

	# skip setup if they aren't running mysqld or want an option that stops mysqld
	if [ "$1" = 'mysqld' ] && ! _mysql_want_help "$@"; then
		mysql_note "Init script for MySQL Server started."

		mysql_check_config "$@"
		# Load various environment variables
		mysql_setup_env "$@"
		mysql_create_db_directories "$@"

		# If container is started as root user, restart as dedicated mysql user
		if [ "$(id -u)" = "0" ]; then
			mysql_note "Switching to dedicated user 'mysql'"
			exec gosu mysql "$BASH_SOURCE" "$@"
		fi

		# there's no database, so it needs to be initialized
		if [ -z "$DATABASE_ALREADY_EXISTS" ]; then
			mysql_verify_minimum_env

			if [ ! -z "${MYSQL_INITDB_SCRIPTS}" ]; then
			  # check dir permissions to reduce likelihood of half-initialized database
			  ls ${MYSQL_INITDB_SCRIPTS}/ > /dev/null
			fi

			mysql_init_database_dir "$@"

			mysql_note "Starting temporary server"
			mysql_temp_server_start "$@"
			mysql_note "Temporary server started."

			mysql_socket_fix
			mysql_setup_db

			if [ "${MYSQL_CLUSTER_MODE}" == "replication" ]; then
				mysql_setup_replication_user
				if [ "${MYSQL_MASTER_NODE}" != "true" ]; then
					mysql_setup_replication
				fi
			elif [ "${MYSQL_CLUSTER_MODE}" == "group_replication" ]; then
				mysql_setup_group_replication_user
				mysql_setup_group_replication
				# Bootstrap cannot be done here for head.  When it is start group replication
				# with bootstrap group ON, it needs to be running for other member to join.
				# Bootstrap needs to be done after the service starting (or with bootstrap group flag
				# and start replication on boot flag ON)
			fi

			if [ "${MYSQL_MASTER_NODE}" == "true" ]; then
				mysql_setup_user_db
				if [ ! -z "${MYSQL_INITDB_SCRIPTS}" ]; then
					mysql_process_init_files ${MYSQL_INITDB_SCRIPTS}/*
				fi
			fi

			mysql_expire_root_user

			mysql_note "Stopping temporary server"
			mysql_temp_server_stop
			mysql_note "Temporary server stopped"

			echo
			mysql_note "MySQL init process done. Ready for start up."
			echo
		else
			mysql_socket_fix
		fi
	fi
	#  Use this as init script
	# exec "$@"
}

# If we are sourced from elsewhere, don't perform any further actions
if ! _is_sourced; then
	_main "$@"
fi
