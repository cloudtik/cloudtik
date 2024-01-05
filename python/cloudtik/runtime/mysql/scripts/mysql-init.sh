#!/bin/bash
set -eo pipefail

# If set, Bash allows filename patterns which match no files
# to expand to a null string, rather than themselves.
shopt -s nullglob

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Load mysql functions
. "$BIN_DIR"/mysql.sh

# check to see if this file is being run or sourced from another script
_is_sourced() {
	# https://unix.stackexchange.com/a/215279
	[ "${#FUNCNAME[@]}" -ge 2 ] \
		&& [ "${FUNCNAME[0]}" = '_is_sourced' ] \
		&& [ "${FUNCNAME[1]}" = 'source' ]
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
		mysql_set_start_replication_on_boot
	fi
	#  Use this as init script
	# exec "$@"
}

# If we are sourced from elsewhere, don't perform any further actions
if ! _is_sourced; then
	_main "$@"
fi
