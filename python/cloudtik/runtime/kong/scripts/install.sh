#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

KONG_VERSION=3.4

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

install_database_tools() {
    which psql > /dev/null \
      || (sudo apt-get -qq update -y > /dev/null; \
        sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install postgresql-client -y > /dev/null)
}

install_kong() {
    if ! command -v kong &> /dev/null
    then
        # WARNING: Kong cannot coexists with APISIX. Both install openresty
        echo "deb [trusted=yes] https://download.konghq.com/gateway-3.x-ubuntu-$(lsb_release -sc)/ default all" \
          | sudo tee /etc/apt/sources.list.d/kong.list >/dev/null

        sudo apt-get -qq update -y > /dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq -y \
            kong=${KONG_VERSION}.\* > /dev/null
        result=$?
        sudo rm -f /etc/apt/sources.list.d/kong.list
        if [ $result -ne 0 ]; then
            echo "Kong installation failed."
            exit 1
        fi
    fi
}

set_head_option "$@"
install_database_tools
install_kong
clean_install
