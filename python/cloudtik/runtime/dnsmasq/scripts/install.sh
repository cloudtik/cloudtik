#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

install_dnsmasq() {
    if ! command -v dnsmasq &> /dev/null
    then
        sudo apt-get -qq update -y > /dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq -y dnsmasq > /dev/null
        result=$?
        if [ $result -ne 0 ]; then
            echo "Dnsmasq installation failed."
            exit 1
        fi
    fi
}

set_head_option "$@"
install_dnsmasq
clean_install
