#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export HADOOP_VERSION=3.3.1

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

# JDK install function
. "$ROOT_DIR"/common/scripts/jdk-install.sh

# Hadoop install function
. "$ROOT_DIR"/common/scripts/hadoop-install.sh

download_hadoop_cloud_jars() {
    HADOOP_TOOLS_LIB=${HADOOP_HOME}/share/hadoop/tools/lib
    HADOOP_HDFS_LIB=${HADOOP_HOME}/share/hadoop/hdfs/lib

    GCS_HADOOP_CONNECTOR="gcs-connector-hadoop3-latest.jar"
    if [ ! -f "${HADOOP_TOOLS_LIB}/${GCS_HADOOP_CONNECTOR}" ]; then
        # Download gcs-connector to ${HADOOP_HOME}/share/hadoop/tools/lib/* for gcp cloud storage support
        wget -q -nc -P "${HADOOP_TOOLS_LIB}" \
          https://storage.googleapis.com/hadoop-lib/gcs/${GCS_HADOOP_CONNECTOR}
    fi

    # Copy Jetty Utility jars from HADOOP_HDFS_LIB to HADOOP_TOOLS_LIB for Azure cloud storage support
    JETTY_UTIL_JARS=('jetty-util-ajax-[0-9]*[0-9].v[0-9]*[0-9].jar' 'jetty-util-[0-9]*[0-9].v[0-9]*[0-9].jar')
    for jar in ${JETTY_UTIL_JARS[@]};
    do
	    find "${HADOOP_HDFS_LIB}" -name $jar | xargs -i cp {} "${HADOOP_TOOLS_LIB}";
    done
}

install_hadoop_with_cloud_jars() {
    # Download jars are possible long running tasks and should be done on install step instead of configure step.
    download_hadoop_cloud_jars
}

set_head_option "$@"
install_jdk
install_hadoop
install_hadoop_with_cloud_jars
