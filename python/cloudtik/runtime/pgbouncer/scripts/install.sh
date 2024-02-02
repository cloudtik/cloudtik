#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export PG_MAJOR=15

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime
export PGBOUNCER_HOME=$RUNTIME_PATH/pgbouncer

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh
. "$ROOT_DIR"/common/scripts/postgres-install.sh

install_pgbouncer() {
    if ! command -v pgbouncer &> /dev/null
    then
        install_postgres_repository
	      # install
        sudo apt-get -qq update -y >/dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq -y \
              pgbouncer "postgresql-client-$PG_MAJOR" >/dev/null
        result=$?
        uninstall_postgres_repository
        if [ $result -ne 0 ]; then
            echo "PgBouncer installation failed."
            exit 1
        fi
    fi
}

set_head_option "$@"
install_pgbouncer
clean_install
