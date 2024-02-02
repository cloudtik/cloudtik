#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export PG_MAJOR=15

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime
export PGPOOL_HOME=$RUNTIME_PATH/pgpool

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh
. "$ROOT_DIR"/common/scripts/postgres-install.sh

install_pgpool() {
    if ! command -v pgpool &> /dev/null
    then
        install_postgres_repository
	      # install
        sudo apt-get -qq update -y >/dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq -y \
              pgpool2 libpgpool2 \
              "postgresql-$PG_MAJOR-pgpool2" >/dev/null
        result=$?
        uninstall_postgres_repository
        if [ $result -ne 0 ]; then
            echo "Pgpool installation failed."
            exit 1
        fi
    fi
}

set_head_option "$@"
install_pgpool
clean_install
