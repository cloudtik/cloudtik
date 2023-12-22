#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

HAPROXY_VERSION=2.8

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

install_haproxy() {
    if ! command -v haproxy &> /dev/null
    then
        sudo apt-get -qq update -y > /dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq --no-install-recommends software-properties-common -y > /dev/null \
          && sudo add-apt-repository ppa:vbernat/haproxy-${HAPROXY_VERSION} -y > /dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq haproxy=${HAPROXY_VERSION}.\* -y > /dev/null
        result=$?
        if [ $result -ne 0 ]; then
            echo "HAProxy installation failed."
            exit 1
        fi
    fi
}

set_head_option "$@"
install_haproxy
clean_install
