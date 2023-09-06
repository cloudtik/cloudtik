#!/bin/bash

# Current bin directory
BIN_DIR=`dirname "$0"`
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

# Hadoop cloud credential configuration functions
. "$BIN_DIR"/hadoop-cloud-credential.sh

function prepare_base_conf() {
    source_dir=$(dirname "${BIN_DIR}")/conf
    output_dir=/tmp/hadoop/conf
    rm -rf  $output_dir
    mkdir -p $output_dir
    cp -r $source_dir/* $output_dir

    # Make copy for local and remote HDFS
    cp $output_dir/hadoop/core-site.xml $output_dir/hadoop/core-site-local.xml
    sed -i "s!{%fs.default.name%}!{%local.fs.default.name%}!g" \
      $output_dir/hadoop/core-site-local.xml
    cp $output_dir/hadoop/core-site.xml $output_dir/hadoop/core-site-remote.xml
    sed -i "s!{%fs.default.name%}!{%remote.fs.default.name%}!g" \
      $output_dir/hadoop/core-site-remote.xml

    cd $output_dir
}

function check_hadoop_installed() {
    if [ ! -n "${HADOOP_HOME}" ]; then
        echo "Hadoop is not installed for HADOOP_HOME environment variable is not set."
        exit 1
    fi
}

function check_hdfs_storage() {
    if [ ! -z  "${HDFS_NAMENODE_URI}" ];then
        HDFS_STORAGE="true"
    else
        HDFS_STORAGE="false"
    fi
}

function update_cloud_storage_credential_config() {
    # update hadoop credential config
    update_credential_config_for_provider
}

function update_config_for_local_hdfs() {
    HADOOP_FS_DEFAULT="hdfs://${HEAD_ADDRESS}:9000"
    sed -i "s!{%fs.default.name%}!${HADOOP_FS_DEFAULT}!g" `grep "{%fs.default.name%}" -rl ./`

    # Still update credential config for cloud provider storage in the case of explict usage
    update_cloud_storage_credential_config
}

function update_config_for_hdfs() {
    # configure namenode uri for core-site.xml
    HADOOP_FS_DEFAULT="${HDFS_NAMENODE_URI}"
    sed -i "s!{%fs.default.name%}!${HADOOP_FS_DEFAULT}!g" `grep "{%fs.default.name%}" -rl ./`

    # Still update credential config for cloud provider storage in the case of explict usage
    update_cloud_storage_credential_config
}

function update_config_for_aws() {
    HADOOP_FS_DEFAULT="s3a://${AWS_S3_BUCKET}"
    sed -i "s!{%fs.default.name%}!${HADOOP_FS_DEFAULT}!g" `grep "{%fs.default.name%}" -rl ./`

    update_cloud_storage_credential_config
}

function update_config_for_gcp() {
    HADOOP_FS_DEFAULT="gs://${GCP_GCS_BUCKET}"
    sed -i "s!{%fs.default.name%}!${HADOOP_FS_DEFAULT}!g" `grep "{%fs.default.name%}" -rl ./`

    update_cloud_storage_credential_config
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

    HADOOP_FS_DEFAULT="${AZURE_SCHEMA}://${AZURE_CONTAINER}@${AZURE_STORAGE_ACCOUNT}.${AZURE_ENDPOINT}.core.windows.net"
    sed -i "s!{%fs.default.name%}!${HADOOP_FS_DEFAULT}!g" `grep "{%fs.default.name%}" -rl ./`

    update_cloud_storage_credential_config
}

function update_config_for_aliyun() {
    HADOOP_FS_DEFAULT="oss://${ALIYUN_OSS_BUCKET}"
    sed -i "s!{%fs.default.name%}!${HADOOP_FS_DEFAULT}!g" `grep "{%fs.default.name%}" -rl ./`
    sed -i "s!{%fs.oss.endpoint%}!${ALIYUN_OSS_INTERNAL_ENDPOINT}!g" `grep "{%fs.oss.endpoint%}" -rl ./`

    update_cloud_storage_credential_config
}

function update_config_for_huaweicloud() {
    HADOOP_FS_DEFAULT="obs://${HUAWEICLOUD_OBS_BUCKET}"
    sed -i "s!{%fs.default.name%}!${HADOOP_FS_DEFAULT}!g" `grep "{%fs.default.name%}" -rl ./`
    sed -i "s!{%fs.obs.endpoint.property%}!${HUAWEICLOUD_OBS_ENDPOINT}!g" `grep "{%fs.obs.endpoint.property%}" -rl ./`

    update_cloud_storage_credential_config
}

function update_config_for_hadoop_default() {
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

function update_config_for_hadoop() {
    check_hdfs_storage
    set_cloud_storage_provider
    update_config_for_hadoop_default
    update_local_storage_config

    if [ "${cloud_storage_provider}" != "none" ];then
        cp -r ${output_dir}/hadoop/${cloud_storage_provider}/core-site.xml ${HADOOP_HOME}/etc/hadoop/
    else
        # hdfs without cloud storage
        cp -r ${output_dir}/hadoop/core-site.xml ${HADOOP_HOME}/etc/hadoop/
    fi
}

function configure_hadoop() {
    prepare_base_conf
    update_config_for_hadoop
}

set_head_option "$@"
check_hadoop_installed
set_head_address
configure_hadoop

exit 0
