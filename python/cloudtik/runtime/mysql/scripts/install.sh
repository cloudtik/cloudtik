#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export MYSQL_MAJOR=8.0
export MYSQL_VERSION=8.0.*debian11

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime
export MYSQL_HOME=$RUNTIME_PATH/mysql

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

install_mysql() {
    if ! command -v mysqld &> /dev/null
    then
        # download the signing key
        # pub   rsa4096 2023-10-23 [SC] [expires: 2025-10-22]
        #       BCA4 3417 C3B4 85DD 128E  C6D4 B7B3 B788 A8D3 785C
        # uid           [ unknown] MySQL Release Engineering <mysql-build@oss.oracle.com>
        # sub   rsa4096 2023-10-23 [E] [expires: 2025-10-22]
        key='BCA4 3417 C3B4 85DD 128E C6D4 B7B3 B788 A8D3 785C'; \
        export GNUPGHOME="$(mktemp -d)"; \
        sudo gpg --batch --keyserver keyserver.ubuntu.com --recv-keys "$key" >/dev/null 2>&1; \
        sudo mkdir -p /etc/apt/keyrings; \
        sudo gpg --batch --export "$key" | sudo tee /etc/apt/keyrings/mysql.gpg >/dev/null; \
        sudo gpgconf --kill all; \
        rm -rf "$GNUPGHOME"
        echo "deb [ signed-by=/etc/apt/keyrings/mysql.gpg ] http://repo.mysql.com/apt/debian/ bullseye mysql-8.0" \
          | sudo tee /etc/apt/sources.list.d/mysql.list >/dev/null

	      # install
        { \
          echo mysql-community-server mysql-community-server/data-dir select ''; \
          echo mysql-community-server mysql-community-server/root-pass password ''; \
          echo mysql-community-server mysql-community-server/re-root-pass password ''; \
          echo mysql-community-server mysql-community-server/remove-test-db select false; \
        } | sudo debconf-set-selections \
        && sudo apt-get -qq update -y >/dev/null \
        && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq -y \
          mysql-community-client="${MYSQL_VERSION}" \
          mysql-community-server-core="${MYSQL_VERSION}" >/dev/null
        result=$?
        sudo rm -f /etc/apt/sources.list.d/mysql.list
        if [ $result -ne 0 ]; then
            echo "MySQL installation failed."
            exit 1
        fi
    fi
}

set_head_option "$@"
install_mysql
clean_install
