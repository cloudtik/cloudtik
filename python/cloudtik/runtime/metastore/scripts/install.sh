#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export HADOOP_VERSION=3.3.1
export HIVE_VERSION=3.1.2

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

# JDK install function
. "$ROOT_DIR"/common/scripts/jdk-install.sh

# Hadoop install function
. "$ROOT_DIR"/common/scripts/hadoop-install.sh

install_database_tools() {
    which mysql > /dev/null \
      || (sudo apt-get -qq update -y > /dev/null; \
          sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install mysql-client -y > /dev/null)

    which psql > /dev/null \
      || (sudo apt-get -qq update -y > /dev/null; \
          sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install postgresql-client -y > /dev/null)
}

install_hive_metastore() {
    # install hive metastore
    export METASTORE_HOME=$RUNTIME_PATH/hive-metastore

    if [ ! -d "${METASTORE_HOME}" ]; then
        mkdir -p $RUNTIME_PATH
        (cd $RUNTIME_PATH \
          && wget -q --show-progress \
            https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/${HIVE_VERSION}/hive-standalone-metastore-${HIVE_VERSION}-bin.tar.gz -O hive-standalone-metastore.tar.gz \
          && mkdir -p "$METASTORE_HOME" \
          && tar --extract --file hive-standalone-metastore.tar.gz --directory "$METASTORE_HOME" --strip-components 1 --no-same-owner \
          && rm -f hive-standalone-metastore.tar.gz)
        if [ $? -ne 0 ]; then
            echo "Metastore installation failed."
            exit 1
        fi
        echo "export METASTORE_HOME=$METASTORE_HOME">> ${USER_HOME}/.bashrc
        # TODO: download only the driver needed
        wget -q https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar -P $METASTORE_HOME/lib/
        wget -q https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -P $METASTORE_HOME/lib/
    fi
}

set_head_option "$@"

install_jdk
install_hadoop
install_database_tools
install_hive_metastore
clean_install
