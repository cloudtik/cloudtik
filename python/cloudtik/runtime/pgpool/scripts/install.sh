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

install_pgpool() {
    if ! command -v postgres &> /dev/null
    then
        # download the signing key
        # wget -O - -q https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
        # echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" \
        #     | sudo tee /etc/apt/sources.list.d/postgres.list

        set -e; \
        key='B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8'; \
        export GNUPGHOME="$(mktemp -d)"; \
        sudo gpg --batch --keyserver keyserver.ubuntu.com --recv-keys "$key" >/dev/null 2>&1; \
        sudo mkdir -p /usr/local/share/keyrings/; \
        sudo gpg --batch --export --armor "$key" | sudo tee /usr/local/share/keyrings/postgres.gpg.asc >/dev/null; \
        sudo gpgconf --kill all; \
        rm -rf "$GNUPGHOME"
        echo "deb [ signed-by=/usr/local/share/keyrings/postgres.gpg.asc ] http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" \
            | sudo tee /etc/apt/sources.list.d/postgres.list >/dev/null

	      # install
        sudo apt-get -qq update -y >/dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq -y \
              pgpool2 libpgpool2 \
              "postgresql-$PG_MAJOR-pgpool2" >/dev/null
        result=$?
        sudo rm -f /etc/apt/sources.list.d/postgres.list
        if [ $result -ne 0 ]; then
            echo "Pgpool installation failed."
            exit 1
        fi
    fi
}

set_head_option "$@"
install_pgpool
clean_install
