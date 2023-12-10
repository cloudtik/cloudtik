#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

# Hadoop cloud credential configuration functions
. "$BIN_DIR"/hadoop-cloud-credential.sh

prepare_base_conf() {
    source_dir=$(dirname "${BIN_DIR}")/conf
    output_dir=/tmp/hadoop/conf
    rm -rf  $output_dir
    mkdir -p $output_dir
    cp -r $source_dir/* $output_dir

    # Make copy for local and remote HDFS
    cp $output_dir/hadoop/core-site.xml $output_dir/hadoop/core-site-local.xml
    cp $output_dir/hadoop/core-site.xml $output_dir/hadoop/core-site-remote.xml

    # Make copy for local and remote MinIO
    cp $output_dir/hadoop/core-site-minio.xml $output_dir/hadoop/core-site-minio-local.xml
    cp $output_dir/hadoop/core-site-minio.xml $output_dir/hadoop/core-site-minio-remote.xml
}

check_hadoop_installed() {
    if [ ! -n "${HADOOP_HOME}" ]; then
        echo "Hadoop is not installed for HADOOP_HOME environment variable is not set."
        exit 1
    fi
}

set_hdfs_storage() {
    if [ ! -z  "${HDFS_NAMENODE_URI}" ];then
        REMOTE_HDFS_STORAGE="true"
    else
        REMOTE_HDFS_STORAGE="false"
    fi

    if [ "$HDFS_ENABLED" == "true" ]; then
        LOCAL_HDFS_STORAGE="true"
    else
        LOCAL_HDFS_STORAGE="false"
    fi
}

set_minio_storage() {
    if [ ! -z  "${MINIO_ENDPOINT_URI}" ];then
        REMOTE_MINIO_STORAGE="true"
    else
        REMOTE_MINIO_STORAGE="false"
    fi

    # In cluster local MinIO storage is not supported because
    # it is not ready to use during head node starting process.
}

set_cluster_storage() {
    set_hdfs_storage
    set_minio_storage
}

update_config_for_local_hdfs() {
    if [ "${cloud_storage_provider}" != "none" ];then
        HADOOP_CORE_SITE=$output_dir/hadoop/${cloud_storage_provider}/core-site.xml
    else
        HADOOP_CORE_SITE=$output_dir/hadoop/core-site.xml
    fi
    HADOOP_FS_DEFAULT="hdfs://${HEAD_IP_ADDRESS}:9000"
    sed -i "s!{%fs.default.name%}!${HADOOP_FS_DEFAULT}!g" $HADOOP_CORE_SITE

    if [ "${cloud_storage_provider}" != "none" ]; then
        # Still update credential config for cloud provider storage in the case of explict usage
        update_cloud_storage_credential_config
    fi
}

update_config_for_hdfs() {
    if [ "${cloud_storage_provider}" != "none" ]; then
        HADOOP_CORE_SITE=$output_dir/hadoop/${cloud_storage_provider}/core-site.xml
    else
        HADOOP_CORE_SITE=$output_dir/hadoop/core-site.xml
    fi
    # configure namenode uri for core-site.xml
    HADOOP_FS_DEFAULT="${HDFS_NAMENODE_URI}"
    sed -i "s!{%fs.default.name%}!${HADOOP_FS_DEFAULT}!g" $HADOOP_CORE_SITE

    if [ "${cloud_storage_provider}" != "none" ]; then
        # Still update credential config for cloud provider storage in the case of explict usage
        update_cloud_storage_credential_config
    fi
}

update_config_for_minio() {
    HADOOP_CORE_SITE=$output_dir/hadoop/core-site-minio.xml
    HADOOP_FS_DEFAULT="s3a://${MINIO_BUCKET}"
    sed -i "s!{%fs.default.name%}!${HADOOP_FS_DEFAULT}!g" $HADOOP_CORE_SITE

    FS_S3A_ENDPOINT="${MINIO_ENDPOINT_URI}"
    sed -i "s!{%fs.s3a.endpoint%}!${FS_S3A_ENDPOINT}!g" $HADOOP_CORE_SITE

    update_minio_storage_credential_config
}

update_config_for_aws() {
    HADOOP_CORE_SITE=$output_dir/hadoop/${cloud_storage_provider}/core-site.xml
    HADOOP_FS_DEFAULT="s3a://${AWS_S3_BUCKET}"
    sed -i "s!{%fs.default.name%}!${HADOOP_FS_DEFAULT}!g" $HADOOP_CORE_SITE

    update_cloud_storage_credential_config
}

update_config_for_gcp() {
    HADOOP_CORE_SITE=$output_dir/hadoop/${cloud_storage_provider}/core-site.xml
    HADOOP_FS_DEFAULT="gs://${GCP_GCS_BUCKET}"
    sed -i "s!{%fs.default.name%}!${HADOOP_FS_DEFAULT}!g" $HADOOP_CORE_SITE

    update_cloud_storage_credential_config
}

update_config_for_azure() {
    HADOOP_CORE_SITE=$output_dir/hadoop/${cloud_storage_provider}/core-site.xml
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
    sed -i "s!{%fs.default.name%}!${HADOOP_FS_DEFAULT}!g" $HADOOP_CORE_SITE

    update_cloud_storage_credential_config
}

update_config_for_aliyun() {
    HADOOP_CORE_SITE=$output_dir/hadoop/${cloud_storage_provider}/core-site.xml
    HADOOP_FS_DEFAULT="oss://${ALIYUN_OSS_BUCKET}"
    sed -i "s!{%fs.default.name%}!${HADOOP_FS_DEFAULT}!g" $HADOOP_CORE_SITE
    sed -i "s!{%fs.oss.endpoint%}!${ALIYUN_OSS_INTERNAL_ENDPOINT}!g" $HADOOP_CORE_SITE

    update_cloud_storage_credential_config
}

update_config_for_huaweicloud() {
    HADOOP_CORE_SITE=$output_dir/hadoop/${cloud_storage_provider}/core-site.xml
    HADOOP_FS_DEFAULT="obs://${HUAWEICLOUD_OBS_BUCKET}"
    sed -i "s!{%fs.default.name%}!${HADOOP_FS_DEFAULT}!g" $HADOOP_CORE_SITE
    sed -i "s!{%fs.obs.endpoint.property%}!${HUAWEICLOUD_OBS_ENDPOINT}!g" $HADOOP_CORE_SITE

    update_cloud_storage_credential_config
}

update_config_for_local_storage() {
    if [ "$REMOTE_HDFS_STORAGE" == "true" ]; then
        update_config_for_hdfs
    elif [ "$REMOTE_MINIO_STORAGE" == "true" ]; then
        update_config_for_minio
    elif [ "$LOCAL_HDFS_STORAGE" == "true" ]; then
        update_config_for_local_hdfs
    fi
}

update_config_for_hadoop_default() {
    if [ "${HADOOP_DEFAULT_CLUSTER}" == "true" ]; then
        update_config_for_local_storage
        if [ ! -z "${HADOOP_CORE_SITE}" ]; then
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
    else
        update_config_for_local_storage
    fi
}

update_nfs_dump_dir() {
    # set nfs gateway dump dir
    data_disk_dir=$(get_first_data_disk_dir)
    if [ -z "$data_disk_dir" ]; then
        nfs_dump_dir="/tmp/.hdfs-nfs"
    else
        nfs_dump_dir="$data_disk_dir/tmp/.hdfs-nfs"
    fi
    sed -i "s!{%dfs.nfs3.dump.dir%}!${nfs_dump_dir}!g" ${output_dir}/hadoop/hdfs-site.xml
}

update_local_storage_config_remote_hdfs() {
    REMOTE_HDFS_CONF_DIR=${HADOOP_HOME}/etc/remote
    # copy the existing hadoop conf
    mkdir -p ${REMOTE_HDFS_CONF_DIR}
    cp -r ${HADOOP_HOME}/etc/hadoop/* ${REMOTE_HDFS_CONF_DIR}/

    HADOOP_CORE_SITE=${output_dir}/hadoop/core-site-remote.xml
    fs_default_dir="${HDFS_NAMENODE_URI}"
    sed -i "s!{%fs.default.name%}!${fs_default_dir}!g" $HADOOP_CORE_SITE

    # override with remote hdfs conf
    cp $HADOOP_CORE_SITE ${REMOTE_HDFS_CONF_DIR}/core-site.xml
    cp -r ${output_dir}/hadoop/hdfs-site.xml ${REMOTE_HDFS_CONF_DIR}/
}

update_local_storage_config_local_hdfs() {
    LOCAL_HDFS_CONF_DIR=${HADOOP_HOME}/etc/local
    # copy the existing hadoop conf
    mkdir -p ${LOCAL_HDFS_CONF_DIR}
    cp -r ${HADOOP_HOME}/etc/hadoop/* ${LOCAL_HDFS_CONF_DIR}/

    HADOOP_CORE_SITE=${output_dir}/hadoop/core-site-local.xml
    fs_default_dir="hdfs://${HEAD_IP_ADDRESS}:9000"
    sed -i "s!{%fs.default.name%}!${fs_default_dir}!g" $HADOOP_CORE_SITE

    # override with local hdfs conf
    cp $HADOOP_CORE_SITE ${LOCAL_HDFS_CONF_DIR}/core-site.xml
    cp -r ${output_dir}/hadoop/hdfs-site.xml ${LOCAL_HDFS_CONF_DIR}/
}

update_local_storage_config_remote_minio() {
    REMOTE_MINIO_CONF_DIR=${HADOOP_HOME}/etc/remote
    # copy the existing hadoop conf
    mkdir -p ${REMOTE_MINIO_CONF_DIR}
    cp -r ${HADOOP_HOME}/etc/hadoop/* ${REMOTE_MINIO_CONF_DIR}/

    HADOOP_CORE_SITE=${output_dir}/hadoop/core-site-minio-remote.xml
    HADOOP_CREDENTIAL_HOME=${REMOTE_MINIO_CONF_DIR}
    HADOOP_CREDENTIAL_NAME=credential-remote.jceks

    fs_default_dir="s3a://${MINIO_BUCKET}"
    sed -i "s!{%fs.default.name%}!${fs_default_dir}!g" $HADOOP_CORE_SITE
    FS_S3A_ENDPOINT="${MINIO_ENDPOINT_URI}"
    sed -i "s!{%fs.s3a.endpoint%}!${FS_S3A_ENDPOINT}!g" $HADOOP_CORE_SITE

    update_minio_storage_credential_config

    cp $HADOOP_CORE_SITE ${REMOTE_MINIO_CONF_DIR}/core-site.xml
}

update_local_storage_config() {
    update_nfs_dump_dir

    if [ "${REMOTE_HDFS_STORAGE}" == "true" ]; then
        update_local_storage_config_remote_hdfs
    elif [ "${REMOTE_MINIO_STORAGE}" == "true" ]; then
        update_local_storage_config_remote_minio
    fi

    if [ "${LOCAL_HDFS_STORAGE}" == "true" ]; then
        update_local_storage_config_local_hdfs
    fi
}

update_config_for_hadoop() {
    HADOOP_CORE_SITE=""
    HADOOP_FS_DEFAULT=""

    set_cluster_storage
    set_cloud_storage_provider
    update_config_for_hadoop_default

    sed -i "s!{%hadoop.fs.default%}!${HADOOP_FS_DEFAULT}!g" ${output_dir}/hadoop-fs-default
    cp ${output_dir}/hadoop-fs-default ${HADOOP_HOME}/etc/hadoop/hadoop-fs-default
    cp $HADOOP_CORE_SITE ${HADOOP_HOME}/etc/hadoop/core-site.xml

    update_local_storage_config
}

configure_hadoop() {
    prepare_base_conf
    update_config_for_hadoop
}

set_head_option "$@"
check_hadoop_installed
set_head_address
configure_hadoop

exit 0
