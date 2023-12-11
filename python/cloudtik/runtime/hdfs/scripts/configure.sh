#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    source_dir=$(dirname "${BIN_DIR}")/conf
    output_dir=/tmp/hdfs/conf
    rm -rf  $output_dir
    mkdir -p $output_dir
    cp -r $source_dir/* $output_dir
}

check_hadoop_installed() {
    if [ ! -n "${HADOOP_HOME}" ]; then
        echo "Hadoop is not installed for HADOOP_HOME environment variable is not set."
        exit 1
    fi
}

get_first_dfs_dir() {
    local data_disk_dir=$(get_first_data_disk_dir)
    if [ -z "$data_disk_dir" ]; then
        data_dir="${HADOOP_HOME}/data/dfs"
    else
        data_dir="$data_disk_dir/dfs"
    fi
    echo "${data_dir}"
}

update_hdfs_data_disks_config() {
    local hdfs_nn_dirs=""
    local hdfs_dn_dirs=""
    if [ -d "/mnt/cloudtik" ]; then
        for data_disk in /mnt/cloudtik/*; do
            [ -d "$data_disk" ] || continue
            local data_dir="$data_disk/dfs"
            if [ "$HDFS_FORCE_CLEAN" == "true" ]; then
                sudo rm -rf "$data_dir"
            fi
            if [ -z "$hdfs_nn_dirs" ]; then
                hdfs_nn_dirs="$data_dir/nn"
            else
                hdfs_nn_dirs="$hdfs_nn_dirs,$data_dir/nn"
            fi
            if [ -z "$hdfs_dn_dirs" ]; then
                hdfs_dn_dirs="$data_dir/dn"
            else
                hdfs_dn_dirs="$hdfs_dn_dirs,$data_dir/dn"
            fi
        done
    fi

    # if no disks mounted
    if [ -z "$hdfs_nn_dirs" ]; then
        if [ "$HDFS_FORCE_CLEAN" == "true" ]; then
            sudo rm -rf "${HADOOP_HOME}/data/dfs"
        fi
        hdfs_nn_dirs="${HADOOP_HOME}/data/dfs/nn"
        hdfs_dn_dirs="${HADOOP_HOME}/data/dfs/dn"
    fi

    sed -i "s!{%dfs.namenode.name.dir%}!${hdfs_nn_dirs}!g" ${HDFS_SITE_CONFIG}
    sed -i "s!{%dfs.datanode.data.dir%}!${hdfs_dn_dirs}!g" ${HDFS_SITE_CONFIG}
}

update_proxy_user_for_current_user() {
    CURRENT_SYSTEM_USER=$(whoami)

    if [ "${CURRENT_SYSTEM_USER}" != "root" ]; then
        HADOOP_PROXY_USER_PROPERTIES="<property>\n\
        <name>hadoop.proxyuser.${CURRENT_SYSTEM_USER}.groups</name>\n\
        <value>*</value>\n\
    </property>\n\
    <property>\n\
        <name>hadoop.proxyuser.${CURRENT_SYSTEM_USER}.hosts</name>\n\
        <value>*</value>\n\
    </property>"
        sed -i "s#{%hadoop.proxyuser.properties%}#${HADOOP_PROXY_USER_PROPERTIES}#g" ${CORE_SITE_CONFIG}
    else
        sed -i "s#{%hadoop.proxyuser.properties%}#""#g" ${CORE_SITE_CONFIG}
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
    sed -i "s!{%dfs.nfs3.dump.dir%}!${nfs_dump_dir}!g" ${HDFS_SITE_CONFIG}
}

configure_hdfs() {
    prepare_base_conf
    mkdir -p ${HADOOP_HOME}/logs

    CORE_SITE_CONFIG=${output_dir}/hadoop/core-site.xml
    HDFS_SITE_CONFIG=${output_dir}/hadoop/hdfs-site.xml

    fs_default_dir="hdfs://${HEAD_IP_ADDRESS}:9000"
    sed -i "s!{%fs.default.name%}!${fs_default_dir}!g" ${CORE_SITE_CONFIG}

    update_proxy_user_for_current_user
    update_hdfs_data_disks_config
    update_nfs_dump_dir

    HDFS_CONF_DIR=${HADOOP_HOME}/etc/hdfs
    # copy the existing hadoop conf
    mkdir -p ${HDFS_CONF_DIR}
    cp -r  ${HADOOP_HOME}/etc/hadoop/* ${HDFS_CONF_DIR}/
    # override hdfs conf
    cp ${CORE_SITE_CONFIG} ${HDFS_CONF_DIR}/core-site.xml
    cp ${HDFS_SITE_CONFIG} ${HDFS_CONF_DIR}/hdfs-site.xml

    # configure the default hadoop to HDFS, if there are other components configure
    # Hadoop, it will override this file
    cp ${CORE_SITE_CONFIG} ${HADOOP_HOME}/etc/hadoop/core-site.xml
    cp ${HDFS_SITE_CONFIG} ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml

    if [ $IS_HEAD_NODE == "true" ]; then
        # format only once if there is no force format flag
        local dfs_dir=$(get_first_dfs_dir)
        HDFS_INIT_FILE=${dfs_dir}/.initialized
        if [ ! -f "${HDFS_INIT_FILE}" ]; then
            export HADOOP_CONF_DIR=${HDFS_CONF_DIR}
            # Stop namenode in case it was running left from last try
            ${HADOOP_HOME}/bin/hdfs --daemon stop namenode > /dev/null 2>&1
            # Format hdfs once
            ${HADOOP_HOME}/bin/hdfs --loglevel WARN namenode -format -force
            if [ $? -eq 0 ]; then
                mkdir -p "${dfs_dir}"
                touch "${HDFS_INIT_FILE}"
            fi
        fi
    fi
}

set_head_option "$@"
check_hadoop_installed
set_head_address
configure_hdfs

exit 0
