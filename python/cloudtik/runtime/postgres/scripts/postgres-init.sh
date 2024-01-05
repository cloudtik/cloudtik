#!/usr/bin/env bash
set -Eeo pipefail

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Load postgres functions
. "$BIN_DIR"/postgres.sh
. "$BIN_DIR"/repmgr.sh

# check to see if this file is being run or sourced from another script
_is_sourced() {
	# https://unix.stackexchange.com/a/215279
	[ "${#FUNCNAME[@]}" -ge 2 ] \
		&& [ "${FUNCNAME[0]}" = '_is_sourced' ] \
		&& [ "${FUNCNAME[1]}" = 'source' ]
}

_main() {
  # if first arg looks like a flag, assume we want to run postgres server
  if [ "${1:0:1}" = '-' ]; then
    set -- postgres "$@"
  fi

  if [ "$1" = 'postgres' ] && ! _pg_want_help "$@"; then
    postgres_setup_env
    # setup data directories and permissions (when run as root)
    postgres_create_db_directories
    if [ "$(id -u)" = '0' ]; then
      # then restart script as postgres user
      exec gosu postgres "$BASH_SOURCE" "$@"
    fi

    # log all commands outputs
    CLOUDTIK_SCRIPT_DEBUG=true
    if [ "${POSTGRES_ROLE}" == "primary" ]; then
      # only run initialization on an empty data directory
      if [ -z "$DATABASE_ALREADY_EXISTS" ]; then
        postgres_verify_minimum_env

        if [ ! -z "${POSTGRES_INITDB_SCRIPTS}" ]; then
          # check dir permissions to reduce likelihood of half-initialized database
          ls ${POSTGRES_INITDB_SCRIPTS}/ > /dev/null
        fi

        postgres_init_database_dir
        pg_setup_hba_conf "$@"
        if [ "${POSTGRES_REPMGR_ENABLED}" == "true" ]; then
          repmgr_setup_hba_conf "$@"
        fi

        # PGPASSWORD is required for psql when authentication is required for 'local' connections via pg_hba.conf and is otherwise harmless
        # e.g. when '--auth=md5' or '--auth-local=md5' is used in POSTGRES_INITDB_ARGS
        export PGPASSWORD="${PGPASSWORD:-$POSTGRES_PASSWORD}"
        postgres_temp_server_start "$@"

        postgres_setup_db
        postgres_setup_replication_user

        if [ "${POSTGRES_REPMGR_ENABLED}" == "true" ]; then
          repmgr_create_repmgr_user
          repmgr_create_repmgr_db
        fi

        postgres_init_db_and_user
        if [ ! -z "${POSTGRES_INITDB_SCRIPTS}" ]; then
          postgres_process_init_files ${POSTGRES_INITDB_SCRIPTS}/*
        fi

        postgres_temp_server_stop
        unset PGPASSWORD

        cat <<-'EOM'

PostgreSQL init process complete; ready for start up.

EOM
      else
        cat <<-'EOM'

PostgreSQL Database directory appears to contain a database; Skipping initialization

EOM
      fi
    else
      # standby role
      if [ "${POSTGRES_REPMGR_ENABLED}" == "true" ]; then
        repmgr_wait_primary_node || exit 1
        repmgr_rewind
      else
        postgres_clone_primary
      fi
    fi

    if [ "${POSTGRES_REPMGR_ENABLED}" == "true" ]; then
      repmgr_configure_preload
    fi
    postgres_setup_synchronous_standby
  fi
}

if ! _is_sourced; then
	_main "$@"
fi
