#!/bin/bash

# Current bin directory
BIN_DIR=`dirname "$0"`
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime

# Hadoop cloud credential configuration functions
. "$ROOT_DIR"/common/scripts/hadoop-cloud-credential.sh

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

function prepare_base_conf() {
    source_dir=$(dirname "${BIN_DIR}")/conf
    output_dir=/tmp/flink/conf
    rm -rf  $output_dir
    mkdir -p $output_dir
    cp -r $source_dir/* $output_dir

    # Include hadoop config file for cloud providers
    cp -r "$ROOT_DIR"/common/conf/hadoop $output_dir
    # Make copy for local and remote HDFS
    cp $output_dir/hadoop/core-site.xml $output_dir/hadoop/core-site-local.xml
    sed -i "s!{%fs.default.name%}!{%local.fs.default.name%}!g" $output_dir/hadoop/core-site-local.xml
    cp $output_dir/hadoop/core-site.xml $output_dir/hadoop/core-site-remote.xml
    sed -i "s!{%fs.default.name%}!{%remote.fs.default.name%}!g" $output_dir/hadoop/core-site-remote.xml

    cd $output_dir
}

function check_flink_installed() {
    if [ ! -n "${HADOOP_HOME}" ]; then
        echo "Hadoop is not installed for HADOOP_HOME environment variable is not set."
        exit 1
    fi

    if [ ! -n "${FLINK_HOME}" ]; then
        echo "Flink is not installed for FLINK_HOME environment variable is not set."
        exit 1
    fi
}

function set_resources_for_flink() {
    # For Head Node
    if [ $IS_HEAD_NODE == "true" ];then
        flink_taskmanager_cores=$(cat ~/cloudtik_bootstrap_config.yaml | jq '."runtime"."flink"."flink_resource"."flink_taskmanager_cores"')
        flink_taskmanager_memory=$(cat ~/cloudtik_bootstrap_config.yaml | jq '."runtime"."flink"."flink_resource"."flink_taskmanager_memory"')M
        flink_jobmanager_memory=$(cat ~/cloudtik_bootstrap_config.yaml | jq '."runtime"."flink"."flink_resource"."flink_jobmanager_memory"')M
    fi
}

function check_hdfs_storage() {
    if [ -n  "${HDFS_NAMENODE_URI}" ];then
        HDFS_STORAGE="true"
    else
        HDFS_STORAGE="false"
    fi
}

function update_cloud_storage_credential_config() {
    # update hadoop credential config
    update_credential_config_for_provider
}

function update_config_for_flink_dirs() {
    sed -i "s!{%flink.state.checkpoints.dir%}!${checkpoints_dir}!g" `grep "{%flink.state.checkpoints.dir%}" -rl ./`
    sed -i "s!{%flink.state.savepoints.dir%}!${savepoints_dir}!g" `grep "{%flink.state.savepoints.dir%}" -rl ./`
    sed -i "s!{%flink.historyserver.archive.fs.dir%}!${historyserver_archive_dir}!g" `grep "{%flink.historyserver.archive.fs.dir%}" -rl ./`
}

function update_config_for_local_hdfs() {
    fs_default_dir="hdfs://${HEAD_ADDRESS}:9000"
    sed -i "s!{%fs.default.name%}!${fs_default_dir}!g" `grep "{%fs.default.name%}" -rl ./`

    # Still update credential config for cloud provider storage in the case of explict usage
    update_cloud_storage_credential_config

    checkpoints_dir="${fs_default_dir}/${PATH_CHECKPOINTS}"
    savepoints_dir="${fs_default_dir}/${PATH_SAVEPOINTS}"
    historyserver_archive_dir="${fs_default_dir}/${PATH_HISTORY_SERVER}"

    update_config_for_flink_dirs
}

function update_config_for_hdfs() {
    # configure namenode uri for core-site.xml
    fs_default_dir="${HDFS_NAMENODE_URI}"
    sed -i "s!{%fs.default.name%}!${fs_default_dir}!g" `grep "{%fs.default.name%}" -rl ./`

    # Still update credential config for cloud provider storage in the case of explict usage
    update_cloud_storage_credential_config

    # checkpoints dir
    checkpoints_dir="${fs_default_dir}/${PATH_CHECKPOINTS}"
    savepoints_dir="${fs_default_dir}/${PATH_SAVEPOINTS}"
    historyserver_archive_dir="${fs_default_dir}/${PATH_HISTORY_SERVER}"

    update_config_for_flink_dirs
}

function update_config_for_aws() {
    fs_default_dir="s3a://${AWS_S3_BUCKET}"
    sed -i "s!{%fs.default.name%}!${fs_default_dir}!g" `grep "{%fs.default.name%}" -rl ./`

    update_cloud_storage_credential_config

    # checkpoints dir
    if [ -z "${AWS_S3_BUCKET}" ]; then
        checkpoints_dir="file:///tmp/flink-checkpoints"
        savepoints_dir="file:///tmp/flink-savepoints"
        historyserver_archive_dir="file:///tmp/history-server"
    else
        checkpoints_dir="${fs_default_dir}/${PATH_CHECKPOINTS}"
        savepoints_dir="${fs_default_dir}/${PATH_SAVEPOINTS}"
        historyserver_archive_dir="${fs_default_dir}/${PATH_HISTORY_SERVER}"
    fi

    update_config_for_flink_dirs
}

function update_config_for_gcp() {
    fs_default_dir="gs://${GCP_GCS_BUCKET}"
    sed -i "s!{%fs.default.name%}!${fs_default_dir}!g" `grep "{%fs.default.name%}" -rl ./`

    update_cloud_storage_credential_config

    # checkpoints dir
    if [ -z "${GCP_GCS_BUCKET}" ]; then
        checkpoints_dir="file:///tmp/flink-checkpoints"
        savepoints_dir="file:///tmp/flink-savepoints"
        historyserver_archive_dir="file:///tmp/history-server"
    else
        checkpoints_dir="${fs_default_dir}/${PATH_CHECKPOINTS}"
        savepoints_dir="${fs_default_dir}/${PATH_SAVEPOINTS}"
        historyserver_archive_dir="${fs_default_dir}/${PATH_HISTORY_SERVER}"
    fi

    update_config_for_flink_dirs
}

function update_config_for_azure() {
    if [ "$AZURE_STORAGE_TYPE" == "blob" ];then
        AZURE_SCHEMA="wasbs"
        AZURE_ENDPOINT="blob"
    else
        # Default to datalake
        # Must be Azure storage kind must be blob (Azure Blob Storage) or datalake (Azure Data Lake Storage Gen 2)
        AZURE_SCHEMA="abfs"
        AZURE_ENDPOINT="dfs"
    fi

    fs_default_dir="${AZURE_SCHEMA}://${AZURE_CONTAINER}@${AZURE_STORAGE_ACCOUNT}.${AZURE_ENDPOINT}.core.windows.net"
    sed -i "s!{%fs.default.name%}!${fs_default_dir}!g" `grep "{%fs.default.name%}" -rl ./`

    update_cloud_storage_credential_config

    # checkpoints dir
    if [ -z "${AZURE_CONTAINER}" ]; then
        checkpoints_dir="file:///tmp/flink-checkpoints"
        savepoints_dir="file:///tmp/flink-savepoints"
        historyserver_archive_dir="file:///tmp/history-server"
    else
        checkpoints_dir="${fs_default_dir}/${PATH_CHECKPOINTS}"
        savepoints_dir="${fs_default_dir}/${PATH_SAVEPOINTS}"
        historyserver_archive_dir="${fs_default_dir}/${PATH_HISTORY_SERVER}"
    fi

    update_config_for_flink_dirs
}

function update_config_for_aliyun() {
    fs_default_dir="oss://${ALIYUN_OSS_BUCKET}"
    sed -i "s!{%fs.default.name%}!${fs_default_dir}!g" `grep "{%fs.default.name%}" -rl ./`

    update_cloud_storage_credential_config

    # checkpoints dir
    if [ -z "${ALIYUN_OSS_BUCKET}" ]; then
        checkpoints_dir="file:///tmp/flink-checkpoints"
        savepoints_dir="file:///tmp/flink-savepoints"
        historyserver_archive_dir="file:///tmp/history-server"
    else
        checkpoints_dir="${fs_default_dir}/${PATH_CHECKPOINTS}"
        savepoints_dir="${fs_default_dir}/${PATH_SAVEPOINTS}"
        historyserver_archive_dir="${fs_default_dir}/${PATH_HISTORY_SERVER}"
    fi

    update_config_for_flink_dirs
}

function update_config_for_huaweicloud() {
    fs_default_dir="obs://${HUAWEICLOUD_OBS_BUCKET}"
    sed -i "s!{%fs.default.name%}!${fs_default_dir}!g" `grep "{%fs.default.name%}" -rl ./`

    update_cloud_storage_credential_config

    # checkpoints dir
    if [ -z "${HUAWEICLOUD_OBS_BUCKET}" ]; then
        checkpoints_dir="file:///tmp/flink-checkpoints"
        savepoints_dir="file:///tmp/flink-savepoints"
        historyserver_archive_dir="file:///tmp/history-server"
    else
        checkpoints_dir="${fs_default_dir}/${PATH_CHECKPOINTS}"
        savepoints_dir="${fs_default_dir}/${PATH_SAVEPOINTS}"
        historyserver_archive_dir="${fs_default_dir}/${PATH_HISTORY_SERVER}"
    fi

    update_config_for_flink_dirs
}

function update_config_for_hadoop_storage() {
    if [ "${HADOOP_DEFAULT_CLUSTER}" == "true" ]; then
        if [ "$HDFS_STORAGE" == "true" ]; then
            update_config_for_hdfs
            return 0
        elif [ "$HDFS_ENABLED" == "true" ]; then
            update_config_for_local_hdfs
            return 0
        fi
    fi

    if [ "${cloud_storage_provider}" == "aws" ]; then
        update_config_for_aws
    elif [ "${cloud_storage_provider}" == "azure" ]; then
        update_config_for_azure
    elif [ "${cloud_storage_provider}" == "gcp" ]; then
        update_config_for_gcp
    elif [ "${cloud_storage_provider}" == "aliyun" ]; then
        update_config_for_aliyun
    elif [ "${cloud_storage_provider}" == "huaweicloud" ]; then
        update_config_for_huaweicloud
    elif [ "$HDFS_STORAGE" == "true" ]; then
        update_config_for_hdfs
    elif [ "$HDFS_ENABLED" == "true" ]; then
        update_config_for_local_hdfs
    fi
}

function update_nfs_dump_dir() {
    # set nfs gateway dump dir
    data_disk_dir=$(get_first_data_disk_dir)
    if [ -z "$data_disk_dir" ]; then
        nfs_dump_dir="/tmp/.hdfs-nfs"
    else
        nfs_dump_dir="$data_disk_dir/tmp/.hdfs-nfs"
    fi
    sed -i "s!{%dfs.nfs3.dump.dir%}!${nfs_dump_dir}!g" `grep "{%dfs.nfs3.dump.dir%}" -rl ./`
}

function update_local_storage_config_remote_hdfs() {
    REMOTE_HDFS_CONF_DIR=${HADOOP_HOME}/etc/remote
    # copy the existing hadoop conf
    mkdir -p ${REMOTE_HDFS_CONF_DIR}
    cp -r ${HADOOP_HOME}/etc/hadoop/* ${REMOTE_HDFS_CONF_DIR}/

    fs_default_dir="${HDFS_NAMENODE_URI}"
    sed -i "s!{%remote.fs.default.name%}!${fs_default_dir}!g" ${output_dir}/hadoop/core-site-remote.xml

    # override with remote hdfs conf
    cp ${output_dir}/hadoop/core-site-remote.xml ${REMOTE_HDFS_CONF_DIR}/core-site.xml
    cp -r ${output_dir}/hadoop/hdfs-site.xml ${REMOTE_HDFS_CONF_DIR}/
}

function update_local_storage_config_local_hdfs() {
    LOCAL_HDFS_CONF_DIR=${HADOOP_HOME}/etc/local
    # copy the existing hadoop conf
    mkdir -p ${LOCAL_HDFS_CONF_DIR}
    cp -r ${HADOOP_HOME}/etc/hadoop/* ${LOCAL_HDFS_CONF_DIR}/

    fs_default_dir="hdfs://${HEAD_ADDRESS}:9000"
    sed -i "s!{%local.fs.default.name%}!${fs_default_dir}!g" ${output_dir}/hadoop/core-site-local.xml

    # override with local hdfs conf
    cp ${output_dir}/hadoop/core-site-local.xml ${LOCAL_HDFS_CONF_DIR}/core-site.xml
    cp -r ${output_dir}/hadoop/hdfs-site.xml ${LOCAL_HDFS_CONF_DIR}/
}

function update_local_storage_config() {
    update_nfs_dump_dir

    if [ "${HDFS_STORAGE}" == "true" ]; then
        update_local_storage_config_remote_hdfs
    fi
    if [ "${HDFS_ENABLED}" == "true" ]; then
        update_local_storage_config_local_hdfs
    fi
}

function update_config_for_storage() {
    PATH_CHECKPOINTS="shared/flink-checkpoints"
    PATH_SAVEPOINTS="shared/flink-savepoints"
    PATH_HISTORY_SERVER="shared/history-server"

    check_hdfs_storage
    set_cloud_storage_provider
    update_config_for_hadoop_storage
    update_local_storage_config

    if [ "${cloud_storage_provider}" != "none" ];then
        cp -r ${output_dir}/hadoop/${cloud_storage_provider}/core-site.xml ${HADOOP_HOME}/etc/hadoop/
    else
        # Possible hdfs without cloud storage
        cp -r ${output_dir}/hadoop/core-site.xml ${HADOOP_HOME}/etc/hadoop/
    fi
}

function update_flink_runtime_config() {
    if [ $IS_HEAD_NODE == "true" ];then
        sed -i "s/{%flink.taskmanager.numberOfTaskSlots%}/${flink_taskmanager_cores}/g" `grep "{%flink.taskmanager.numberOfTaskSlots%}" -rl ./`
        sed -i "s/{%flink.taskmanager.memory.process.size%}/${flink_taskmanager_memory}/g" `grep "{%flink.taskmanager.memory.process.size%}" -rl ./`
        sed -i "s/{%flink.jobmanager.memory.process.size%}/${flink_jobmanager_memory}/g" `grep "{%flink.jobmanager.memory.process.size%}" -rl ./`
    fi
}

function update_flink_local_dir() {
    # set flink local dir
    flink_local_dir=$local_dirs
    if [ -z "$flink_local_dir" ]; then
        flink_local_dir="/tmp"
    fi
    sed -i "s!{%flink.local.dir%}!${flink_local_dir}!g" `grep "{%flink.local.dir%}" -rl ./`
}

function update_metastore_config() {
    # To be improved for external metastore cluster
    FLINK_DEFAULTS=${output_dir}/flink/flink-conf.yaml
    if [ ! -z "$HIVE_METASTORE_URI" ] || [ "$METASTORE_ENABLED" == "true" ]; then
        if [ ! -z "$HIVE_METASTORE_URI" ]; then
            hive_metastore_uris="$HIVE_METASTORE_URI"
        else
            METASTORE_IP=${HEAD_ADDRESS}
            hive_metastore_uris="thrift://${METASTORE_IP}:9083"
        fi

        hive_metastore_version="3.1.2"

        if [ ! -n "${HIVE_HOME}" ]; then
            hive_metastore_jars=maven
        else
            hive_metastore_jars="${HIVE_HOME}/lib/*"
        fi

        sed -i "s!{%flink.hadoop.hive.metastore.uris%}!flink.hadoop.hive.metastore.uris ${hive_metastore_uris}!g" ${FLINK_DEFAULTS}
        sed -i "s!{%flink.sql.hive.metastore.version%}!flink.sql.hive.metastore.version ${hive_metastore_version}!g" ${FLINK_DEFAULTS}
        sed -i "s!{%flink.sql.hive.metastore.jars%}!flink.sql.hive.metastore.jars ${hive_metastore_jars}!g" ${FLINK_DEFAULTS}
    else
        # replace with empty
        sed -i "s/{%flink.hadoop.hive.metastore.uris%}//g" ${FLINK_DEFAULTS}
        sed -i "s/{%flink.sql.hive.metastore.version%}//g" ${FLINK_DEFAULTS}
        sed -i "s/{%flink.sql.hive.metastore.jars%}//g" ${FLINK_DEFAULTS}
    fi
}

function configure_hadoop_and_flink() {
    prepare_base_conf

    sed -i "s/HEAD_ADDRESS/${HEAD_ADDRESS}/g" `grep "HEAD_ADDRESS" -rl ./`

    update_flink_runtime_config
    update_flink_local_dir
    update_config_for_storage

    if [ $IS_HEAD_NODE == "true" ];then
        update_metastore_config
        cp -r ${output_dir}/flink/* ${FLINK_HOME}/conf
    fi
}

function configure_jupyter_for_flink() {
  if [ $IS_HEAD_NODE == "true" ]; then
      mkdir -p ${RUNTIME_PATH}/jupyter/logs

      echo Y | jupyter lab --generate-config;
      # Set default password(cloudtik) for JupyterLab
      sed -i  "1 ic.NotebookApp.password = 'argon2:\$argon2id\$v=19\$m=10240,t=10,p=8\$Y+sBd6UhAyKNsI+/mHsy9g\$WzJsUujSzmotUkblSTpMwCFoOBVSwm7S5oOPzpC+tz8'" ~/.jupyter/jupyter_lab_config.py

      # Set default notebook_dir for JupyterLab
      export JUPYTER_WORKSPACE=${RUNTIME_PATH}/jupyter/notebooks
      mkdir -p $JUPYTER_WORKSPACE
      sed -i  "1 ic.NotebookApp.notebook_dir = '${JUPYTER_WORKSPACE}'" ~/.jupyter/jupyter_lab_config.py
      sed -i  "1 ic.NotebookApp.ip = '${HEAD_ADDRESS}'" ~/.jupyter/jupyter_lab_config.py
  fi
}

set_head_option "$@"
check_flink_installed
set_head_address
set_resources_for_flink
configure_hadoop_and_flink
configure_jupyter_for_flink

exit 0
