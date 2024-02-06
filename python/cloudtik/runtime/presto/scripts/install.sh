#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export PRESTO_VERSION=0.282

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime

# JDK install function
. "$ROOT_DIR"/common/scripts/jdk-install.sh

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

install_tools() {
    which uuid > /dev/null \
      || (sudo apt-get -qq update -y > /dev/null; \
          sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install uuid -y > /dev/null)
}

install_presto() {
    # install Presto
    export PRESTO_HOME=$RUNTIME_PATH/presto

    if [ ! -d "${PRESTO_HOME}" ]; then
        mkdir -p $RUNTIME_PATH
        (cd $RUNTIME_PATH \
          && wget -q --show-progress \
            https://repo1.maven.org/maven2/com/facebook/presto/presto-server/${PRESTO_VERSION}/presto-server-${PRESTO_VERSION}.tar.gz -O presto-server.tar.gz \
          && mkdir -p "$PRESTO_HOME" \
          && tar --extract --file presto-server.tar.gz --directory "$PRESTO_HOME" --strip-components 1 --no-same-owner \
          && rm -f presto-server.tar.gz)
        if [ $? -ne 0 ]; then
            echo "Presto installation failed."
            exit 1
        fi
        echo "export PRESTO_HOME=$PRESTO_HOME">> ${USER_HOME}/.bashrc
        if [ "$IS_HEAD_NODE" == "true" ]; then
            # Download presto cli on head
            (cd $RUNTIME_PATH \
              && wget -q --show-progress \
                https://repo1.maven.org/maven2/com/facebook/presto/presto-cli/${PRESTO_VERSION}/presto-cli-${PRESTO_VERSION}-executable.jar \
              && mv presto-cli-${PRESTO_VERSION}-executable.jar $PRESTO_HOME/bin/presto \
              && chmod +x $PRESTO_HOME/bin/presto)
            echo "export PATH=\$PRESTO_HOME/bin:\$PATH" >> ${USER_HOME}/.bashrc
        fi
    fi
}

set_head_option "$@"
install_jdk
install_tools
install_presto
clean_install
