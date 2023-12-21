#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export GRAFANA_VERSION=10.0.3

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime
export GRAFANA_HOME=$RUNTIME_PATH/grafana

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

install_grafana() {
    if ! command -v grafana &> /dev/null
    then
        deb_arch=$(get_deb_arch)
        mkdir -p $RUNTIME_PATH
        (cd $RUNTIME_PATH \
          && wget -q --show-progress \
            https://dl.grafana.com/oss/release/grafana-${GRAFANA_VERSION}.linux-${deb_arch}.tar.gz -O grafana.tar.gz \
          && mkdir -p "$GRAFANA_HOME" \
          && tar --extract --file grafana.tar.gz --directory "$GRAFANA_HOME" --strip-components 1 --no-same-owner \
          && rm -f grafana.tar.gz)
        if [ $? -ne 0 ]; then
            echo "Grafana installation failed."
            exit 1
        fi
        echo "export GRAFANA_HOME=$GRAFANA_HOME">> ${USER_HOME}/.bashrc
        echo "export PATH=\$GRAFANA_HOME/bin:\$PATH" >> ${USER_HOME}/.bashrc
    fi
}

set_head_option "$@"
install_grafana
clean_install
