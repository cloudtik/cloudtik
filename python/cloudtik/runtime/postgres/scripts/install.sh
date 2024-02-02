#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export PG_MAJOR=15

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime
export POSTGRES_HOME=$RUNTIME_PATH/postgres

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh
. "$ROOT_DIR"/common/scripts/postgres-install.sh

install_postgres() {
    if ! command -v postgres &> /dev/null
    then
        # make the "en_US.UTF-8" locale so postgres will be utf-8 enabled by default
        sudo apt-get -qq update -y >/dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq -y \
              --no-install-recommends locales libnss-wrapper zstd >/dev/null \
          && sudo rm -rf /var/lib/apt/lists/* \
	        && sudo localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 \
          && echo "export LANG=en_US.utf8" >> ${USER_HOME}/.bashrc

        install_postgres_repository
	      # install
        sudo apt-get -qq update -y >/dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq -y \
            --no-install-recommends postgresql-common >/dev/null \
          && sudo sed -ri 's/#(create_main_cluster) .*$/\1 = false/' /etc/postgresql-common/createcluster.conf \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq -y \
              --no-install-recommends "postgresql-$PG_MAJOR" "postgresql-client-$PG_MAJOR" libpq-dev \
              "postgresql-$PG_MAJOR-repmgr" >/dev/null
        result=$?
        uninstall_postgres_repository
        if [ $result -ne 0 ]; then
            echo "Postgres installation failed."
            exit 1
        fi
        echo "export PATH=/usr/lib/postgresql/$PG_MAJOR/bin:\$PATH" >> ${USER_HOME}/.bashrc
    fi
}

set_head_option "$@"
install_postgres
clean_install
