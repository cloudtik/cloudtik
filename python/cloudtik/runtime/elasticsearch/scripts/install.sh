#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export ELASTICSEARCH_VERSION="8.11.3"

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime
export ELASTICSEARCH_HOME=$RUNTIME_PATH/elasticsearch

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

install_elasticsearch() {
    if [ ! -d "${ELASTICSEARCH_HOME}" ]; then
        mkdir -p $RUNTIME_PATH
        (cd $RUNTIME_PATH \
          && wget -q --show-progress \
            https://artifacts-no-kpi.elastic.co/downloads/elasticsearch/elasticsearch-${ELASTICSEARCH_VERSION}-linux-$(arch).tar.gz -O elasticsearch.tar.gz \
          && mkdir -p "$ELASTICSEARCH_HOME" \
          && tar --extract --file elasticsearch.tar.gz --directory "$ELASTICSEARCH_HOME" --strip-components 1 --no-same-owner \
          && rm -f elasticsearch.tar.gz)
        if [ $? -ne 0 ]; then
            echo "ElasticSearch installation failed."
            exit 1
        fi
        echo "export ELASTICSEARCH_HOME=$ELASTICSEARCH_HOME">> ${USER_HOME}/.bashrc
        echo "export PATH=\$ELASTICSEARCH_HOME/bin:\$PATH" >> ${USER_HOME}/.bashrc
    fi
}

set_head_option "$@"
install_elasticsearch
