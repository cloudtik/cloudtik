#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/flink/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_flink_installed() {
    if [ ! -n "${HADOOP_HOME}" ]; then
        echo "Hadoop is not installed."
        exit 1
    fi

    if [ ! -n "${FLINK_HOME}" ]; then
        echo "Flink is not installed."
        exit 1
    fi
}

set_resources_for_flink() {
    # For Head Node
    if [ "$IS_HEAD_NODE" == "true" ]; then
        local -r bootstrap_config="~/cloudtik_bootstrap_config.yaml"
        flink_taskmanager_cores=$(cat "$bootstrap_config" | jq '."runtime"."flink"."flink_resource"."flink_taskmanager_cores"')
        flink_taskmanager_memory=$(cat "$bootstrap_config" | jq '."runtime"."flink"."flink_resource"."flink_taskmanager_memory"')M
        flink_jobmanager_memory=$(cat "$bootstrap_config" | jq '."runtime"."flink"."flink_resource"."flink_jobmanager_memory"')M
    fi
}

update_flink_storage_dirs() {
    PATH_CHECKPOINTS="flink-checkpoints"
    PATH_SAVEPOINTS="flink-savepoints"
    PATH_HISTORY_SERVER="history-server"

    HADOOP_FS_DEFAULT_FILE=${HADOOP_HOME}/etc/hadoop/hadoop-fs-default
    if [ -f "${HADOOP_FS_DEFAULT_FILE}" ]; then
        . ${HADOOP_FS_DEFAULT_FILE}
    fi

    # checkpoints dir
    if [ -z "${HADOOP_FS_DEFAULT}" ]; then
        checkpoints_dir="file:///tmp/${PATH_CHECKPOINTS}"
        savepoints_dir="file:///tmp/${PATH_SAVEPOINTS}"
        historyserver_archive_dir="file:///tmp/${PATH_HISTORY_SERVER}"
    else
        checkpoints_dir="${fs_default_dir}/shared/${PATH_CHECKPOINTS}"
        savepoints_dir="${fs_default_dir}/shared/${PATH_SAVEPOINTS}"
        historyserver_archive_dir="${fs_default_dir}/shared/${PATH_HISTORY_SERVER}"
    fi

    sed -i "s!{%flink.state.checkpoints.dir%}!${checkpoints_dir}!g" ${FLINK_CONFIG_FILE}
    sed -i "s!{%flink.state.savepoints.dir%}!${savepoints_dir}!g" ${FLINK_CONFIG_FILE}
    sed -i "s!{%flink.historyserver.archive.fs.dir%}!${historyserver_archive_dir}!g" ${FLINK_CONFIG_FILE}
}

update_flink_runtime_config() {
    if [ "$IS_HEAD_NODE" == "true" ];then
        sed -i "s/{%flink.taskmanager.numberOfTaskSlots%}/${flink_taskmanager_cores}/g" ${FLINK_CONFIG_FILE}
        sed -i "s/{%flink.taskmanager.memory.process.size%}/${flink_taskmanager_memory}/g" ${FLINK_CONFIG_FILE}
        sed -i "s/{%flink.jobmanager.memory.process.size%}/${flink_jobmanager_memory}/g" ${FLINK_CONFIG_FILE}
    fi
}

update_flink_local_dir() {
    # set flink local dir
    flink_local_dir=$local_dirs
    if [ -z "$flink_local_dir" ]; then
        flink_local_dir="/tmp"
    fi
    sed -i "s!{%flink.local.dir%}!${flink_local_dir}!g" ${FLINK_CONFIG_FILE}
}

update_metastore_config() {
    # To be improved for external metastore cluster
    if [ ! -z "$HIVE_METASTORE_URI" ] || [ "$METASTORE_ENABLED" == "true" ]; then
        if [ ! -z "$HIVE_METASTORE_URI" ]; then
            hive_metastore_uris="$HIVE_METASTORE_URI"
        else
            METASTORE_HOST=${HEAD_HOST_ADDRESS}
            hive_metastore_uris="thrift://${METASTORE_HOST}:9083"
        fi

        hive_metastore_version="3.1.2"

        if [ ! -n "${HIVE_HOME}" ]; then
            hive_metastore_jars=maven
        else
            hive_metastore_jars="${HIVE_HOME}/lib/*"
        fi

        sed -i "s!{%flink.hadoop.hive.metastore.uris%}!flink.hadoop.hive.metastore.uris ${hive_metastore_uris}!g" ${FLINK_CONFIG_FILE}
        sed -i "s!{%flink.sql.hive.metastore.version%}!flink.sql.hive.metastore.version ${hive_metastore_version}!g" ${FLINK_CONFIG_FILE}
        sed -i "s!{%flink.sql.hive.metastore.jars%}!flink.sql.hive.metastore.jars ${hive_metastore_jars}!g" ${FLINK_CONFIG_FILE}
    else
        # replace with empty
        sed -i "s/{%flink.hadoop.hive.metastore.uris%}//g" ${FLINK_CONFIG_FILE}
        sed -i "s/{%flink.sql.hive.metastore.version%}//g" ${FLINK_CONFIG_FILE}
        sed -i "s/{%flink.sql.hive.metastore.jars%}//g" ${FLINK_CONFIG_FILE}
    fi
}

configure_flink() {
    prepare_base_conf
    FLINK_CONFIG_FILE=${OUTPUT_DIR}/flink/flink-conf.yaml

    update_flink_runtime_config
    update_flink_local_dir
    update_flink_storage_dirs

    if [ "$IS_HEAD_NODE" == "true" ];then
        update_metastore_config
        cp -r ${OUTPUT_DIR}/flink/* ${FLINK_HOME}/conf
    fi
}

configure_jupyter_for_flink() {
  if [ "$IS_HEAD_NODE" == "true" ]; then
      mkdir -p ${RUNTIME_PATH}/jupyter/logs

      echo Y | jupyter lab --generate-config;
      # Set default password(cloudtik) for JupyterLab
      sed -i  "1 ic.NotebookApp.password = 'argon2:\$argon2id\$v=19\$m=10240,t=10,p=8\$Y+sBd6UhAyKNsI+/mHsy9g\$WzJsUujSzmotUkblSTpMwCFoOBVSwm7S5oOPzpC+tz8'" ~/.jupyter/jupyter_lab_config.py

      # Set default notebook_dir for JupyterLab
      export JUPYTER_WORKSPACE=${RUNTIME_PATH}/jupyter/notebooks
      mkdir -p $JUPYTER_WORKSPACE
      sed -i  "1 ic.NotebookApp.notebook_dir = '${JUPYTER_WORKSPACE}'" ~/.jupyter/jupyter_lab_config.py
      sed -i  "1 ic.NotebookApp.ip = '${NODE_IP_ADDRESS}'" ~/.jupyter/jupyter_lab_config.py
  fi
}

set_head_option "$@"
check_flink_installed
set_head_address
set_node_address
set_resources_for_flink
configure_flink
configure_jupyter_for_flink

exit 0
