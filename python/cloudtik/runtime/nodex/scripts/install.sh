#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export NODEX_VERSION=1.6.1

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime
export NODEX_HOME=$RUNTIME_PATH/nodex

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

install_nodex() {
    if [ ! -f "${NODEX_HOME}/nodex" ]; then
        deb_arch=$(get_deb_arch)
        mkdir -p $RUNTIME_PATH
        (cd $RUNTIME_PATH \
          && wget -q --show-progress \
            https://github.com/prometheus/node_exporter/releases/download/v${NODEX_VERSION}/node_exporter-${NODEX_VERSION}.linux-${deb_arch}.tar.gz -O nodex.tar.gz \
          && mkdir -p "$NODEX_HOME" \
          && tar --extract --file nodex.tar.gz --directory "$NODEX_HOME" --strip-components 1 --no-same-owner \
          && mv $NODEX_HOME/node_exporter $NODEX_HOME/nodex \
          && rm -f nodex.tar.gz)
        if [ $? -ne 0 ]; then
            echo "Nodex installation failed."
            exit 1
        fi
        echo "export NODEX_HOME=$NODEX_HOME">> ${USER_HOME}/.bashrc
    fi
}

set_head_option "$@"
install_nodex
clean_install
