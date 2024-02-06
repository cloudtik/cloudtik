#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export TRINO_VERSION=423

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

# JDK install function
. "$ROOT_DIR"/common/scripts/jdk-install.sh

# Hadoop install function
. "$ROOT_DIR"/common/scripts/hadoop-install.sh

install_tools() {
    which uuid > /dev/null \
      || (sudo apt-get -qq update -y > /dev/null; \
          sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install uuid -y > /dev/null)
}

install_trino() {
    # install Trino
    export TRINO_HOME=$RUNTIME_PATH/trino

    if [ ! -d "${TRINO_HOME}" ]; then
        mkdir -p $RUNTIME_PATH
        (cd $RUNTIME_PATH \
          && wget -q --show-progress \
            https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz -O trino-server.tar.gz \
          && mkdir -p "$TRINO_HOME" \
          && tar --extract --file trino-server.tar.gz --directory "$TRINO_HOME" --strip-components 1 --no-same-owner \
          && rm -f trino-server.tar.gz)
        if [ $? -ne 0 ]; then
            echo "Trino installation failed."
            exit 1
        fi
        echo "export TRINO_HOME=$TRINO_HOME">> ${USER_HOME}/.bashrc

        if [ "$IS_HEAD_NODE" == "true" ]; then
            # Download trino cli on head
            (cd $RUNTIME_PATH \
              && wget -q --show-progress \
                https://repo1.maven.org/maven2/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar \
              && mv trino-cli-${TRINO_VERSION}-executable.jar $TRINO_HOME/bin/trino \
              && chmod +x $TRINO_HOME/bin/trino)
            echo "export PATH=\$TRINO_HOME/bin:\$PATH" >> ${USER_HOME}/.bashrc
        fi
    fi
}

set_head_option "$@"
install_jdk
install_hadoop
install_tools
install_trino
clean_install
